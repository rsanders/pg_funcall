require 'spec_helper'

#
# Test the utility class for calling database functions
#

describe PgFuncall do
  before(:all) do
    ActiveRecord::Base.connection.execute <<-SQL
      CREATE EXTENSION IF NOT EXISTS hstore;

      CREATE OR REPLACE FUNCTION public.dbfspec_polyfunc(arg1 anyelement)
      RETURNS anyelement
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg1;
      END;
      $function$;

      CREATE OR REPLACE FUNCTION public.dbfspec_textfunc(arg1 text, arg2 text)
      RETURNS text
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg1 || ',' || arg2;
      END;
      $function$;

      CREATE OR REPLACE FUNCTION public.dbfspec_intfunc(arg1 integer, arg2 integer)
      RETURNS integer
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg1 + arg2;
      END;
      $function$;

      CREATE OR REPLACE FUNCTION public.dbfspec_arrayfunc(arg integer[])
      RETURNS integer[]
      LANGUAGE plpgsql
      AS $function$
      DECLARE
        result integer[];
        val integer;
      BEGIN
        result := '{}';
        FOREACH val IN ARRAY arg LOOP
          result := array_append(result, val * 2);
        END LOOP;
        RETURN result;
      END;
      $function$;

      CREATE OR REPLACE FUNCTION public.dbfspec_hstorefunc(arg hstore, cay text)
      RETURNS text
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg -> cay;
      END;
      $function$;

      CREATE OR REPLACE FUNCTION public.dbfspec_hstorefunc(arg1 hstore, arg2 hstore)
      RETURNS hstore
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        RETURN arg1 || arg2;
      END;
      $function$;
  SQL

  end

  after(:all) do
    ActiveRecord::Base.connection.execute <<-SQL
      DROP FUNCTION IF EXISTS public.dbfspec_polyfunc(anyelement);
      DROP FUNCTION IF EXISTS public.dbfspec_textfunc(text, text);
      DROP FUNCTION IF EXISTS public.dbfspec_intfunc(integer, integer);
      DROP FUNCTION IF EXISTS public.dbfspec_arrayfunc(integer[]);
      DROP FUNCTION IF EXISTS public.dbfspec_hstorefunc(hstore, text);

    SQL
  end

  let :search_path do
    ActiveRecord::Base.connection.schema_search_path.split(/, ?/)
  end

  context 'introspection' do
    subject { PgFuncall.default_instance }

    context '#search_path' do
      it 'should return the expected search path' do
        subject.search_path.should == search_path
      end
    end
  end

  context 'quoting for inlining into string' do
    subject { PgFuncall.default_instance }
    it 'does not quote integer' do
      subject._quote_param(50).should == "50"
    end

    it 'single-quotes a string' do
      subject._quote_param("foo").should == "'foo'"
    end

    it 'handles single quotes embedded in string' do
      subject._quote_param("ain't misbehavin'").should ==
          "'ain''t misbehavin'''"
    end

    it 'quotes string array properly' do
      subject._quote_param(%w[a b cdef]).should ==
          "ARRAY['a','b','cdef']"
    end

    it 'quotes integer array properly' do
      subject._quote_param([99, 100]).should ==
          "ARRAY[99,100]"
    end

    # XXX: this can be iffy unless there's a clear typecast or
    #  unambiguous function parameter type; arrays must be typed
    #  so it's best to specify the type of empty arrays
    it 'quotes empty array' do
      subject._quote_param([]).should ==
          "ARRAY[]"
    end

    it 'quotes Ruby hash as hstore' do
      subject._quote_param({a: 1, b: :foo}).should ==
          "$$a => 1,b => foo$$::hstore"
    end
  end

  context 'quoting for inclusing in Pg param descriptor' do
    subject { PgFuncall.default_instance }

    it 'does not quote integer' do
      subject._format_param_for_descriptor(50).should == "50"
    end

    it 'single-quotes a string' do
      subject._format_param_for_descriptor("foo").should == "foo"
    end

    it 'handles single quotes embedded in string' do
      subject._format_param_for_descriptor("ain't misbehavin'").should ==
          "ain't misbehavin'"
    end

    it 'quotes string array properly' do
      subject._format_param_for_descriptor(%w[a b cdef]).should ==
          "{a,b,cdef}"
    end

    it 'quotes integer array properly' do
      subject._format_param_for_descriptor([99, 100]).should ==
          "{99,100}"
    end

    # XXX: this can be iffy unless there's a clear typecast or
    #  unambiguous function parameter type; arrays must be typed
    #  so it's best to specify the type of empty arrays
    it 'quotes empty array' do
      subject._format_param_for_descriptor([]).should ==
          "{}"
    end

    it 'quotes Ruby hash as hstore' do
      subject._format_param_for_descriptor({a: 1, b: :foo}).should ==
          "a => 1,b => foo"
    end
  end

  context 'simple call with string return' do
    it 'should return a string as a string' do
      PgFuncall.call_uncast('public.dbfspec_textfunc', "hello", "goodbye").should == 'hello,goodbye'
    end

    it 'should return a polymorphic-cast string as a string' do
      PgFuncall.call_uncast('public.dbfspec_polyfunc',
                      PgFuncall.literal("'hello'::text")).should == 'hello'
    end

    it 'should return a number as a string' do
      PgFuncall.call_uncast('public.dbfspec_intfunc', 55, 100).should == "155"
    end

    it 'should return an array as a string' do
      PgFuncall.call_uncast('public.dbfspec_arrayfunc',
                      PgFuncall.literal('ARRAY[55, 100]::integer[]')).should == '{110,200}'
    end
  end

  context 'call with typecast return' do
    it 'should return a polymorphic-cast string as a string' do
      PgFuncall.call('public.dbfspec_polyfunc',
                      PgFuncall.literal("'hello'::text")).should == 'hello'
    end

    it 'should return a string as a string' do
      PgFuncall.call('public.dbfspec_textfunc', "hello", "goodbye").should == 'hello,goodbye'
    end

    it 'should return a number as a string' do
      PgFuncall.call('public.dbfspec_intfunc', 55, 100).should == 155
    end

    it 'should return a literal array as an array' do
      PgFuncall.call('public.dbfspec_arrayfunc',
                      PgFuncall.literal('ARRAY[55, 100]::integer[]')).
          should == [110, 200]
    end

    it 'should take a Ruby array as a PG array' do
      PgFuncall.call('public.dbfspec_arrayfunc',
                      [30, 92]).should == [60, 184]
    end

    it 'should take a Ruby hash as a PG hstore' do
      PgFuncall.call('public.dbfspec_hstorefunc',
                      {'a' => 'foo', 'b' => 'baz'}, 'b').should == 'baz'
    end

    it 'should return a PG hstore as a Ruby hash ' do
      PgFuncall.call('public.dbfspec_hstorefunc',
                      {'a' => 'foo', 'b' => 'baz'},
                      {'c' => 'cat'}).should == {'a' => 'foo', 'b' => 'baz', 'c' => 'cat'}
    end
  end

  context 'type roundtripping via SELECT' do
    def roundtrip(value)
      PgFuncall.casting_query("SELECT $1;", [value]).first
    end

    context 'numeric types' do
      it 'should return an integer for an integer' do
        roundtrip(3215).should eql(3215)
      end

      it 'should return a float for a float' do
        roundtrip(77.45).should eql(77.45)
      end

      it 'should handle bigdecimal' do
        roundtrip(BigDecimal.new("500.23")).should == BigDecimal.new("500.23")
      end
    end

    context 'textual types' do
      it 'should handle character' do
        roundtrip(?Z).should == "Z"
      end

      it 'should handle UTF-8 string' do
        roundtrip("peter piper picked").should == "peter piper picked"
      end

      it 'should handle binary string' do
        binstring = [*(1..100)].pack('L*')
        puts "encoding is #{binstring.encoding}"
        roundtrip(binstring).should == binstring
      end
    end

    context 'hashes' do
      it 'should handle empty hashes' do
        roundtrip({}).should == {}
      end
      it 'should handle text->text hashes' do
        roundtrip({'a' => 'foo', 'b' => 'baz'}).should == {'a' => 'foo', 'b' => 'baz'}
      end
      it 'should handle hashes with other type values' do
        roundtrip({a: 'foo', b: 750}).should == {'a' => 'foo', 'b' => '750'}
      end
    end

    context 'arrays' do
      it 'should throw exception on untagged empty array' do
        expect { roundtrip([]) }.to raise_error
      end
      it 'should handle wrapped, typed empty arrays' do
        roundtrip(PgFuncall::TypedArray.new([], 'int4')).should == []
      end
      it 'should handle tagged arrays' do
        roundtrip(PgFuncall::TypedArray.new([1,2,2**45], 'int8')).should == [1, 2, 2**45]
      end
      it 'should handle int arrays' do
        roundtrip([1,2,77]).should == [1, 2, 77]
      end
      it 'should handle string arrays' do
        roundtrip(%w[a b longerstring c]).should == ['a', 'b', 'longerstring', 'c']
      end
      it 'should handle arrays of int arrays' do
        pending 'it returns [nil, nil] for reasons unknown - PLS FIX GOOBY'
        roundtrip(PgFuncall::TypedArray.new([[1,2,77], [99, 0, 4]], 'int4[]')).
                      should == [[1,2,77], [99, 0, 4]]
      end

      it 'should handle arrays of txt arrays' do
        roundtrip([['a', 'b'], ['x', 'y']]).should == [['a', 'b'], ['x', 'y']]

      end
    end

    context 'temporal types' do
      let(:now) { Time.now }
      it 'should handle Date' do
        roundtrip(now).should == now
      end
      it 'should handle DateTime' do
        roundtrip(now.to_datetime).should == now.to_datetime
      end
      it 'should handle Time' do
        roundtrip(PgFuncall::PGTime.new('11:45')).should == Time.parse('2000-01-01 11:45:00 UTC')
      end
      it 'should handle interval' do
        roundtrip(PgFuncall::PGTimeInterval.new('1 hour')).should == '01:00:00'
      end
      it 'should handle with time zone'
    end

    context 'network types' do
      it 'should handle IPv4 host without netmask' do
        roundtrip(IPAddr.new("1.2.3.4")).should == IPAddr.new("1.2.3.4")
      end
      it 'should handle IPv4 host expressed as /32' do
        roundtrip(IPAddr.new("1.2.3.4/32")).should == IPAddr.new("1.2.3.4/32")
      end
      it 'should handle IPv4 network' do
        roundtrip(IPAddr.new("1.2.3.4/20")).should == IPAddr.new("1.2.3.4/20")
      end
      it 'should handle IPv6 host' do
        roundtrip(IPAddr.new("2607:f8b0:4002:c01::65")).should == IPAddr.new("2607:f8b0:4002:c01::65")
      end
      it 'should handle IPv6 network' do
        roundtrip(IPAddr.new("2001:db8:abcd:8000::/50")).should == IPAddr.new("2001:db8:abcd:8000::/50")
      end
    end

    context 'misc types' do
      it 'should handle UUIDs' do
        uuid = UUID.new.generate
        roundtrip(PgFuncall::PGUUID.new(uuid)).should == uuid
      end
      it 'should handle OIDs'
      it 'should handle explicitly tagged types'
      it 'should handle untyped literals'
      it 'should handle literals including a cast'
    end

    context 'range types' do
      it 'should handle int4 ranges'
      it 'should handle int8 ranges'
      it 'should handle end-exclusive ranges'
      it 'should handle bignum ranges'
      it 'should handle decimal ranges'
      it 'should handle date ranges'
      it 'should handle time ranges'
      it 'should handle timestamp ranges'
    end

  end

end
