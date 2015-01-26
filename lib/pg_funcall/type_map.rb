require 'active_record'

class PgFuncall
  class TypeMap
    def self.fetch(connection)
      case ActiveRecord.version.segments[0..1]
        when [4,0] then AR40TypeMap.new(connection)
        when [4,1] then AR41TypeMap.new(connection)
        when [4,2] then AR42TypeMap.new(connection)
        else
          raise ArgumentError, "Unsupported ActiveRecord version #{ActiveRecord.version}"
      end
    end
  end

  class AR40TypeMap < TypeMap
    def initialize(connection)
    end

    def lookup_by_oid(oid)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::TYPE_MAP[oid]
    end

    def lookup_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end
  end

  class AR41TypeMap < TypeMap
    def initialize(connection)
      @connection = connection
    end

    def lookup_by_name(name)
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::NAMES[name]
    end

    def lookup_by_oid(oid)
      @connection.instance_variable_get("@type_map")[oid]
    end
  end

  class AR42TypeMap < TypeMap
    def initialize(connection)
      @connection = connection
    end

    def lookup_by_name(name)
      @connection.instance_variable_get("@type_map").lookup(name)
    end

    def lookup_by_oid(oid)
      @connection.instance_variable_get("@type_map").lookup(oid)
    end
  end

end

