require 'spec_helper'
require 'pg_funcall/type_map'

#
# Test the utility class for calling database functions
#

describe PgFuncall::TypeMap do
  subject do
    PgFuncall::TypeMap.fetch(ActiveRecord::Base.connection)
  end

  before(:all) do
    ActiveRecord::Base.connection.execute <<-SQL
      CREATE OR REPLACE FUNCTION public.dbfspec_textfunc(arg1 text, arg2 text)
      RETURNS text
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg1 || ',' || arg2;
      END;
      $function$;
    SQL

    end

    after(:all) do
      ActiveRecord::Base.connection.execute <<-SQL
        DROP FUNCTION IF EXISTS public.dbfspec_textfunc(text, text);
      SQL
    end


  context 'creation' do
    it { should be_a(PgFuncall::TypeMap) }
  end

  context 'lookup' do
    context 'by name' do
      it 'should return the appropriate type for int4' do
        subject.lookup_by_name('int4').should be_a(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Integer)
      end
    end

    context 'by oid' do
      it 'should return the appropriate array type for 17 (bytea)' do
        subject.lookup_by_oid(17).should be_a(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Bytea)
      end

      it 'should return the appropriate array type for 1007 (int4 array)' do
        typobj = subject.lookup_by_oid(1007)
        typobj.should be_a(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Array)
        typobj.subtype.should be_a(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Integer)
      end
    end
  end

  context 'introspection' do
    subject { PgFuncall.new(ActiveRecord::Base.connection).type_map }

    context '#function_types' do
      it 'returns expected types for qualified name' do
        subject.function_types('public.dbfspec_textfunc').
            should == [25, [25, 25]]
      end

      it 'returns same types for unqualified name' do
        subject.function_types('public.dbfspec_textfunc').
            should == subject.function_types('dbfspec_textfunc', PgFuncall.default_instance.search_path)
      end

      it 'throws if non-namespaced function is specified without a search_path' do
        expect { subject.function_types('dbfspec_textfunc', []) }.
          to raise_error
      end
    end
  end
end
