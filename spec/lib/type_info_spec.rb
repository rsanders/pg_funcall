require 'spec_helper'
require 'pg_funcall/type_info'
require 'pg_funcall/type_map'

#
# Test the utility class for calling database functions
#

describe PgFuncall::TypeInfo do
  let :int4row do
    {
      "oid"=>"23",
      "nspname"=>"pg_catalog",
      "typname"=>"int4",
      "typnamespace"=>"11",
      "typowner"=>"10",
      "typlen"=>"4",
      "typbyval"=>"t",
      "typtype"=>"b",
      "typcategory"=>"N",
      "typispreferred"=>"f",
      "typisdefined"=>"t",
      "typdelim"=>",",
      "typrelid"=>"0",
      "typelem"=>"0",
      "typarray"=>"1007",
      "typinput"=>"int4in",
      "typoutput"=>"int4out",
      "typreceive"=>"int4recv",
      "typsend"=>"int4send",
      "typmodin"=>"-",
      "typmodout"=>"-",
      "typanalyze"=>"-",
      "typalign"=>"i",
      "typstorage"=>"p",
      "typnotnull"=>"f",
      "typbasetype"=>"0",
      "typtypmod"=>"-1",
      "typndims"=>"0",
      "typcollation"=>"0",
      "typdefaultbin"=>nil,
      "typdefault"=>nil,
      "typacl"=>nil,
      "nspowner"=>"10",
      "nspacl"=>"{robertsanders=UC/robertsanders,=U/robertsanders}"
    }
  end

  let :pg_connection do
    ActiveRecord::Base.connection.raw_connection
  end

  #
  # Array of string->string hashes representing known Postgres types
  #
  let :all_type_rows do
    pg_connection.query(<<-SQL).to_a
       SELECT pgt.oid, ns.nspname, *
       FROM pg_type as pgt
       JOIN pg_namespace as ns on pgt.typnamespace = ns.oid;
    SQL
  end

  let :types_by_oid do
    {}.tap do |hash|
      all_type_rows.each do |row|
        hash[row['oid'].to_i] = row
      end
    end
  end

  let :types_by_name do
    {}.tap do |hash|
      all_type_rows.each do |row|
        raise "Entry for #{row['oid']} - #{row['typname']} already defined!" if hash.has_key?(row['typname'])
        hash[row['typname']] = row
      end
    end
  end

  let :type_map do
    PgFuncall::TypeMap.fetch(ActiveRecord::Base.connection)
  end

  let :ar_type do
    type_map.lookup_ar_by_oid(row['oid'].to_i)
  end

  # currently inspected row; int4 by default
  let :row do
    int4row
  end

  subject do
    described_class.new(row, ar_type)
  end

  context 'for int4 hardcoded row' do
    it 'should return the correct OID' do
      subject.oid.should == 23
    end
    it 'should return the simple name' do
      subject.name.should == 'int4'
    end
    it 'should return a simple fqname' do
      subject.fqname.should == 'int4'
    end
    it { should be_numeric }
    it { should_not be_temporal }
    it { should_not be_array }
    it 'should have an array type' do
      subject.array_type_oid.should_not be_nil
    end
    it 'should cast a string to integer' do
      subject.cast_from_database('3211').should == 3211
    end
  end

  context 'parsed from all_type_rows' do

  end
end
