require 'active_record'
require 'pg_funcall/type_info'

class PgFuncall
  class FunctionSig
    FTYPE_CACHE = {}

    attr_reader :name, :ret_type, :arg_sigs

    def initialize(name, ret_type, arg_sigs)
      @name     = name.freeze
      @ret_type = ret_type
      @arg_sigs = arg_sigs.sort.freeze
    end

    def ==(other)
      other.name     == @name
      other.ret_type == @ret_type
      other.arg_sigs == @arg_sigs
    end
  end

  class PgType
    def initialize(pginfo, ar_type)
      @pginfo = pginfo
      @ar_type = ar_type
    end

    def to_s
      array ? "#{name}[]" : name
    end
  end

  #
  # See http://www.postgresql.org/docs/9.4/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
  #
  class TypeMap
    def self.fetch(connection, options = {})
      case ActiveRecord.version.segments[0..1]
        when [4,0] then AR40TypeMap.new(connection, options)
        when [4,1] then AR41TypeMap.new(connection, options)
        when [4,2] then AR42TypeMap.new(connection, options)
        else
          raise ArgumentError, "Unsupported ActiveRecord version #{ActiveRecord.version}"
      end
    end


    def initialize(connection, options = {})
      @ftype_cache = {}
      @ar_connection = connection
      @options = options
      @typeinfo         = []
      @typeinfo_by_name = {}
      @typeinfo_by_oid  = {}

      load_types
    end

    def load_types
      res = pg_connection.query <<-SQL
         SELECT pgt.oid, ns.nspname, *
         FROM pg_type as pgt
         JOIN pg_namespace as ns on pgt.typnamespace = ns.oid;
      SQL

      fields = res.fields
      @typeinfo = res.values.map do |values|
        row = Hash[fields.zip(values)]
        TypeInfo.new(row, lookup_ar_by_oid(row['oid'].to_i))
      end

      @typeinfo_by_name.clear
      @typeinfo_by_oid.clear

      @typeinfo.each do |ti|
        @typeinfo_by_name[ti.name] = ti
        @typeinfo_by_oid[ti.oid]   = ti
      end
    end

    def ar_connection
      @ar_connection
    end

    def pg_connection
      @ar_connection.raw_connection
    end

    def type_cast_from_database(value, type)
      type.cast_from_database(value)
    end

    def resolve(oid_or_name)
      if oid_or_name.is_a?(Integer) || (oid_or_name.is_a?(String) && oid_or_name.match(/^[0-9]+$/))
        @typeinfo_by_oid[oid_or_name.to_i]
      elsif oid_or_name.is_a?(String) || oid_or_name.is_a?(Symbol)
        @typeinfo_by_name[oid_or_name.to_s]
      else
        raise ArgumentError, "You must supply a numeric OID or a string Type name"
      end
    end

    def lookup_by_oid(oid)
      lookup_ar_by_oid(oid)
    end

    def lookup_by_name(name)
      lookup_ar_by_name(name)
    end

    #
    # Given a type name, with optional appended [] for array types, find the OID for it.
    #
    def oid_for_type(type, array = false)
      array = type.end_with?('[]')
      qtype = type.gsub(/(\[\])+$/, '')
      pg_connection.query("SELECT oid, typarray from pg_type where typname = '#{qtype}';") do |res|
        return nil if res.ntuples == 0

        if array
          res.getvalue(0,1)
        else
          res.getvalue(0,0)
        end
      end.to_i
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
    def function_types(fn, search_path = @options[:search_path])
      return @ftype_cache[fn] if @ftype_cache[fn]

      parts = fn.split('.')
      info =  if parts.length == 1
                raise ArgumentError, "Must supply search_path for non-namespaced function" unless
                  search_path && search_path.is_a?(Enumerable) && !search_path.empty?
                search_path.map do |ns|
                  res = pg_connection.query(FMETAQUERY % [parts[0], ns])
                  res.ntuples == 1 ? res : nil
                end.compact.first
              else
                pg_connection.query(FMETAQUERY % [parts[1], parts[0]])
              end

      return nil unless info && info.ntuples >= 1

      @ftype_cache[fn] =
          FunctionSig.new(fn,
                          info.getvalue(0,0).to_i,
                          (0..info.ntuples-1).map { |row|
                            info.getvalue(row, 1).split(/ +/).map(&:to_i)
                          })
    end
  end

  class AR40TypeMap < TypeMap
    def lookup_ar_by_oid(oid)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::TYPE_MAP[oid]
    end

    def lookup_ar_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end
  end

  class AR41TypeMap < TypeMap
    def lookup_ar_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end

    def lookup_ar_by_oid(oid)
      @ar_connection.instance_variable_get("@type_map")[oid]
    end
  end

  class AR42TypeMap < TypeMap
    def lookup_ar_by_name(name)
      @ar_connection.instance_variable_get("@type_map").lookup(name)
    end

    def lookup_ar_by_oid(oid)
      @ar_connection.instance_variable_get("@type_map").lookup(oid)
    end

    def type_cast_from_database(value, type)
      type.ar_type.type_cast_from_database(value)
    end
  end
end

