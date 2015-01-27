# used for some query forms, and for mapping types from AR -> Ruby
require 'active_record'

# supported types
require 'uuid'
require 'ipaddr'
require 'ipaddr_extensions'
require 'pg_funcall/ipaddr_monkeys'
require 'pg_funcall/type_map'

class PgFuncall
  module HelperMethods
    [:call_uncast, :call_raw, :call_scalar, :call_returning_array,
     :clear_cache, :call_returning_type, :call_cast, :call, :casting_query].each do |meth|
      define_method(meth) do |*args, &blk|
        PgFuncall.default_instance.__send__(meth, *args, &blk)
      end
    end
  end


  def self.default_instance
    @default_instance ||= PgFuncall.new(ActiveRecord::Base.connection)
  end

  def self.default_instance=(instance)
    @default_instance = instance
  end

  def initialize(connection)
    raise ArgumentError, "Requires ActiveRecord PG connection" unless
        connection.is_a?(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)

    @ar_connection = connection

    clear_cache
  end

  def clear_cache
    (@ftype_cache ||= {}).clear
    @type_map = nil
    true
  end

  #
  # Value wrappers
  #

  module PGTyped
  end

  module PGWritable
    extend(PGTyped)
  end

  module PGReadable
    extend(PGTyped)
  end

  Typed = Struct.new(:value, :type) do
    include PGTyped
    def __pg_type
      type
    end
  end

  class TypedArray < Typed
  end

  class PGTime < Typed
    def initialize(time)
      super(time, 'time')
    end
  end

  class PGTimeInterval < Typed
    def initialize(interval)
      super(interval, 'interval')
    end
  end

  class PGUUID < Typed
    def initialize(uuid)
      super(uuid, 'uuid')
    end

    def self.generate
      PGUUID.initialize(UUID.new.generate)
    end

    def to_s
      self.value
    end
  end

  Literal = Struct.new(:value)

  def self.tag_pg_type(value, tagtype, pgvalue = nil)
    pgvalue ||= value

    # XXX: this is going to blow the method cache every time it runs
    value.class_eval do
      include PGTyped

      define_method(:__pg_value, lambda do
        pgvalue
      end)

      define_method(:__pg_type, lambda do
        tagtype
      end)
    end
    value
  end

  #
  # wrap a value so that it is inserted into the query as-is
  #
  def self.literal(arg)
    Literal.new(arg)
  end

  #
  # Calls Database function with a given set of arguments. Returns result as a string.
  #
  def call_uncast(fn, *args)
    call_raw(fn, *args).rows.first.first
  end
  alias :call_scalar :call_uncast

  def call_returning_array(fn, *args)
    call_raw(fn, *args).rows
  end

  #
  # "Quote", which means to format and quote, a parameter for inclusion into
  # a SQL query as a string.
  #
  def _quote_param(param, type=nil)
    return param.value if param.is_a?(Literal)

    case param
      when Array
        "ARRAY[" + param.map {|p| _quote_param(p)}.join(",") + "]"
      when Set
        _quote_param(param.to_a)
      when Hash
        '$$' + param.map do |k,v|
          "#{k} => #{v}"
        end.join(',') + '$$::hstore'
      else
        ActiveRecord::Base.connection.quote(param)
    end
  end

  #
  # Represent a Ruby object in a string form to be passed as a parameter
  # within a descriptor hash, rather than substituted into a string-form
  # query.
  #
  def _format_param_for_descriptor(param, type=nil)
    return param.value if param.is_a?(Literal)

    case param
      when TypedArray
        _format_param_for_descriptor(param.value, param.type + "[]")
      when Typed
        _format_param_for_descriptor(param.value, param.type)
      when PGTyped
        param.respond_to?(:__pg_value) ?
            param.__pg_value :
            _format_param_for_descriptor(param, type)
      when TrueClass
        'true'
      when FalseClass
        'false'
      when String
        if type == 'bytea' || param.encoding == Encoding::BINARY
          '\x' + param.unpack('C*').map {|x| sprintf("%02X", x)}.join("")
        else
          param
        end
      when Array
        "{" + param.map {|p| _format_param_for_descriptor(p)}.join(",") + "}"
      when IPAddr
        param.to_cidr_string
      when Range
        last_char = param.exclude_end? ? ')' : ']'
        case type
          when 'tsrange', 'tstzrange'
            "[#{param.first.utc},#{param.last.utc}#{last_char}"
          else
            "[#{param.first},#{param.last}#{last_char}"
        end
      when Set
        _format_param_for_descriptor(param.to_a)
      when Hash
        param.map do |k,v|
          "#{k} => #{v}"
        end.join(',')
      else
        ActiveRecord::Base.connection.quote(param)
    end
  end

  def call_raw_inline(fn, *args)
    query = "SELECT #{fn}(" +
         args.map {|arg| _quote_param(arg) }.join(", ") + ") as res;"

    ActiveRecord::Base.connection.exec_query(query,
                                             "calling for DB function #{fn}")
  end

  def call_raw_pg(fn, *args, &blk)
    query = "SELECT #{fn}(" +
         args.map {|arg| _quote_param(arg) }.join(", ") + ") as res;"

    _pg_conn.query(query, &blk).tap do |res|
      PgFuncall._assign_pg_type_map_to_res(res, _pg_conn)
    end
  end

  alias :call_raw :call_raw_inline

  def type_for_typeid(typeid)
    type_map.resolve(typeid.to_i)
  end

  def type_for_name(name)
    type_map.resolve(name)
  end

  def type_map
    @type_map ||= TypeMap.fetch(@ar_connection, search_path: search_path)
  end

  #
  # Force a typecast of the return value
  #
  def call_returning_type(fn, ret_type, *args)
    type_map.type_cast_from_database(call(fn, *args),
                                     type_for_name(ret_type))
  end

  def self._assign_pg_type_map_to_res(res, conn)
    return res

    ## this appears to fail to roundtrip on bytea and date types
    ##
    #if res.respond_to?(:type_map=)
    #  res.type_map = PG::BasicTypeMapForResults.new(conn)
    #end
    #res
  end

  #
  # Take a PGResult and cast the first column of each tuple to the
  # Ruby equivalent of the PG type as described in the PGResult.
  #
  def _cast_pgresult(res)
    PgFuncall._assign_pg_type_map_to_res(res, _pg_conn)
    res.column_values(0).map do |val|
      type_map.type_cast_from_database(val,
                                       type_for_typeid(res.ftype(0)))
    end
  end

  def call_cast(fn, *args)
    fn_sig = type_map.function_types(fn)

    ## TODO: finish this with the new type info class
    # unwrap = fn_sig && type_map.is_scalar_type?(type_map.lookup_by_oid(fn_sig.ret_type))

    call_raw_pg(fn, *args) do |res|
      results = _cast_pgresult(res)
      # unwrap && results.ntuples < 2 ? results.first : results
      results.first
    end
  end

  def _pg_param_descriptors(params)
    params.map do |p|
      pgtype = _pgtype_for_value(p)
      {value: _format_param_for_descriptor(p, pgtype),
       # if we can't find a type, let PG guess
       type:  type_map.oid_for_type(pgtype) || 0,
       format: 0}
    end
  end

  def casting_query(query, params)
    # puts "param descriptors = #{_pg_param_descriptors(params)}.inspect"
    _pg_conn.exec_params(query, _pg_param_descriptors(params)) do |res|
      _cast_pgresult(res)
    end
  end


  def _pgtype_for_value(value)
    case value
      # type-forcing wrapper for arrays
      when TypedArray
        value.type + '[]'

      # type-forcing wrapper
      when Typed
        value.type

      # marker ancestor
      when PGTyped
        value.__pg_type

      when String
        if value.encoding == Encoding::BINARY
          'bytea'
        else
          'text'
        end
      when Fixnum, Bignum
        'int4'
      when Float
        'float4'
      when TrueClass, FalseClass
        'bool'
      when BigDecimal
        'numeric'
      when Hash
        'hstore'
      when UUID
        'uuid'
      when Time, DateTime
        'timestamp'
      when Date
        'date'
      when IPAddr
        if value.host?
          'inet'
        else
          'cidr'
        end
      when Range
        case value.last
          when Fixnum
            if value.last > (2**31)-1
              'int8range'
            else
              'int4range'
            end
          when Bignum then 'int8range'
          when DateTime, Time then 'tsrange'
          when Date then 'daterange'
          when Float, BigDecimal, Numeric then 'numrange'
          else
            raise "Unknown range type: #{value.first.type}"
        end
      when Array, Set
        raise "Empty untyped array" if value.empty?
        _pgtype_for_value(value.first) + '[]'
      else
        'text'
    end
  end

  alias :call :call_cast

  def _ar_conn
    @ar_connection
  end

  def _pg_conn
    _ar_conn.raw_connection
  end

  #
  # Return an array of schema names for the current session's search path
  #
  def search_path
    _pg_conn.query("SHOW search_path;") do |res|
      res.column_values(0).first.split(/, ?/)
    end
  end

  extend(HelperMethods)
end
