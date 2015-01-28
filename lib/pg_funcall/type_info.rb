class PgFuncall::TypeInfo
  attr_accessor :ar_type
  attr_accessor :array_type
  attr_accessor :element_type

  def initialize(row, ar_type = nil)
    @row = {}
    # copy and convert int-looking things to int along the way
    row.each do |key, val|
      @row[key] =
          (val && val.respond_to?(:match) && val.match(/^-?\d+$/)) ? val.to_i : val
    end
    @row.freeze
    @ar_type = ar_type
  end

  # TODO: replace this to not use ar_type
  def cast_from_database(value)
    @ar_type.respond_to?(:type_cast_from_database) ?
        @ar_type.type_cast_from_database(value) :
        @ar_type.type_cast(value)
  end

    #
  # Represent a Ruby object in a string form to be passed as a parameter
  # within a descriptor hash, rather than substituted into a string-form
  # query.
  #
  def _format_param_for_descriptor(param, type=nil)
    return param.value if param.is_a?(PgFuncall::Literal)

    case param
      when PgFuncall::TypedArray
        _format_param_for_descriptor(param.value, param.type + "[]")
      when PgFuncall::Typed
        _format_param_for_descriptor(param.value, param.type)
      when PgFuncall::PGTyped
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

    # TODO: replace this to not use ar_type
  def cast_to_database(value)
    @ar_type.respond_to?(:type_cast_for_database) ?
        @ar_type.type_cast_for_database(value).to_s :
        _format_param_for_descriptor(value, name)
  end

  def name
    @row['typname']
  end

  def namespace
    @row['nspname']
  end

  #
  # Don't fully qualify base types -- this is pretty, but is it wise?
  #
  def fqname
    namespace == 'pg_catalog' ?
        name :
        namespace + '.' + name
  end

  def oid
    @row['oid']
  end

  def category
    @row['typcategory']
  end

  def temporal?
    datetime? || timespan?
  end

  CATEGORY_MAP =
      {'A' => 'array',
       'B' => 'boolean',
       'C' => 'composite',
       'D' => 'datetime',
       'E' => 'enum',
       'G' => 'geometric',
       'I' => 'network_address',
       'N' => 'numeric',
       'P' => 'pseudotype',
       'S' => 'string',
       'T' => 'timespan',
       'U' => 'user_defined',
       'V' => 'bit_string',
       'X' => 'unknown'
      }

  CATEGORY_MAP.each do |typ, name|
    define_method("#{name}?") do
      category == typ
    end
  end

  def category_name
    CATEGORY_MAP[category]
  end

  def element_type_oid
    raise "Can only call on array" unless array?
    @row['typelem']
  end

  def array_type_oid
    @row['typarray']
  end

  def [](element)
    @row[element.to_s]
  end
end
