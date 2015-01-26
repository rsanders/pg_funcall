require 'active_record'

class PgFuncall
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
    end

    def ar_connection
      @ar_connection
    end

    def pg_connection
      @ar_connection.raw_connection
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
      return @ftype_cache[fn] if @ftype_cache.has_key?(fn)

      parts = fn.split('.')
      puts "components is #{parts.inspect}"
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

      return nil unless info && info.ntuples == 1

      # returns an array of [return value type, [arg types]]
      @ftype_cache[fn] = [
          info.getvalue(0,0).to_i,
          info.getvalue(0,1).split(/ +/).map(&:to_i)
      ]
    end
  end

  class AR40TypeMap < TypeMap
    def lookup_by_oid(oid)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::TYPE_MAP[oid]
    end

    def lookup_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end
  end

  class AR41TypeMap < TypeMap
    def lookup_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end

    def lookup_by_oid(oid)
      @ar_connection.instance_variable_get("@type_map")[oid]
    end
  end

  class AR42TypeMap < TypeMap
    def lookup_by_name(name)
      @ar_connection.instance_variable_get("@type_map").lookup(name)
    end

    def lookup_by_oid(oid)
      @ar_connection.instance_variable_get("@type_map").lookup(oid)
    end
  end

end

