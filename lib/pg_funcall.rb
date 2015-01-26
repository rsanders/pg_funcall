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

    @connection = connection

    clear_cache
  end

  def clear_cache
    (@ftype_cache ||= {}).clear
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
  end

  Literal = Struct.new(:value)

  def self.tag_pg_type(value, tagtype, pgvalue = nil)
    pgvalue ||= value

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
        #"ARRAY[" + param.map {|p| _format_param_for_descriptor(p)}.join(",") + "]"
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

    _pg_conn.query(query, &blk)
  end

  alias :call_raw :call_raw_inline

  def _ar_type_for_typeid(typeid)
    type_map.lookup_by_oid(typeid)
  end

  def _ar_type_for_name(name)
    type_map.lookup_by_name(name)
  end

  def type_map
    @type_map ||= TypeMap.fetch(@connection)
  end

  #
  # Force a typecast of the return value
  #
  def call_returning_type(fn, ret_type, *args)
    _ar_type_for_name(ret_type).type_cast(call(fn, *args))
  end

  def _cast_pgresult(res)
    res.column_values(0).map do |val|
      _ar_type_for_typeid(res.ftype(0)).type_cast(val)
    end
  end


  def call_cast(fn, *args)
    call_raw_pg(fn, *args) do |res|
      _cast_pgresult(res).first
    end
  end

  def _oid_for_type(type)
    qtype = type.gsub(/(\[\])+$/, '')
    _pg_conn.query("SELECT oid, typarray from pg_type where typname = '#{qtype}';") do |res|
      return nil if res.ntuples == 0

      if type.end_with?('[]')
        res.getvalue(0,1)
      else
        res.getvalue(0,0)
      end
    end.to_i
  end

  def _pg_param_descriptors(params)
    params.map do |p|
      pgtype = _pgtype_for_value(p)
      {value: _format_param_for_descriptor(p, pgtype),
       # if we can't find a type, let PG guess
       type:  _oid_for_type(pgtype) || 0,
       format: 0}
    end
  end

  def casting_query(query, params)
    puts "param desctiptors = #{_pg_param_descriptors(params)}.inspect"
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
    ActiveRecord::Base.connection
  end

  def _pg_conn
    _ar_conn.raw_connection
  end

  FMETAQUERY = <<-"SQL"
          SELECT prorettype, proargtypes
          FROM pg_proc as pgp
          JOIN pg_namespace as ns on pgp.pronamespace = ns.oid
          WHERE proname = '%s' AND ns.nspname = '%s';
        SQL

  #
  # Query PostgreSQL metadata about function to find its
  # return type and argument types
  #
  def function_types(fn)
    return @ftype_cache[fn] if @ftype_cache.has_key?(fn)

    parts = fn.split('.', 2)
    info = if parts.length == 1
      search_path.map do |ns|
        res = _pg_conn.query(FMETAQUERY % [parts[0], ns])
        res.ntuples == 1 ? res : nil
      end.compact.first
    else
      _pg_conn.query(FMETAQUERY % [parts[1], parts[0]])
    end

    return nil unless info && info.ntuples == 1

    # returns an array of [return value type, [arg types]]
    @ftype_cache[fn] = [
        info.getvalue(0,0).to_i,
        info.getvalue(0,1).split(/ +/).map(&:to_i)
    ]
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
