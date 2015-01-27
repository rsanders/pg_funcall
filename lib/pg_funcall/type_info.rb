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
