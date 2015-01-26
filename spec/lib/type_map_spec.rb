require 'spec_helper'
require 'pg_funcall/type_map'

#
# Test the utility class for calling database functions
#

describe PgFuncall::TypeMap do
  subject do
    PgFuncall::TypeMap.fetch(ActiveRecord::Base.connection)
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
end
