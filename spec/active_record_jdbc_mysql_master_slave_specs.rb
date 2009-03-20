require File.expand_path("#{File.dirname(__FILE__)}/spec_helper")
require File.expand_path("#{File.dirname(__FILE__)}/../lib/active_record_jdbc_mysql_master_slave")

module ActiveRecord::ConnectionAdapters
  describe JdbcAdapter do

    # need JdbcConnection class defined
    class JdbcConnection
    end

    describe "with_master" do
      it "should default to false" do
        JdbcAdapter.master_only.should_not ==(true)
      end

      it "should execute a block with master_only true and restore the original value after" do
        JdbcAdapter::with_master do
          JdbcAdapter.master_only.should ==(true)
        end

        JdbcAdapter.master_only.should_not ==(true)
      end
    end

    describe "with_master_or_slave" do
      it "should permit overriding of master_only and restore the original setting after" do
        JdbcAdapter::with_master do
          JdbcAdapter.master_only.should ==(true)

          JdbcAdapter::with_master_or_slave do
            JdbcAdapter.master_only.should_not ==(true)
          end
        end

        JdbcAdapter.master_only.should_not ==(true)
      end
    end

    # rspec makes it really hard to test initializers... since u can only
    # set expectations on methods of an existing instance. we create
    # a FooAdapter which is easier to construct than a real JdbcAdapter
    # and figure out whether the initializer performed correctly 
    # retrospectively
    class FooAdapter
      def initialize( *args )
      end
      
      def _execute( *args )
      end
      
      include JdbcAdapterMysqlMasterSlaveMethods
    end
    
    DEFAULT_FOO_ADAPTER_OPTS = { 
      :adapter=>"mysql",
      :master_only => false,
      :auto_commit => true,
      :select? => true,
      :read_only => false
    }

    # omfg
    def make_foo_adapter(opts={})
      opts = DEFAULT_FOO_ADAPTER_OPTS.merge(opts)

      JdbcAdapter.stub!(:master_only).and_return( opts[:master_only] )
      JdbcConnection.stub!(:select?).and_return( opts[:select?] )
      
      c = mock("connection")
      c.stub!( :auto_commit ).and_return( opts[:auto_commit] )

      # fake the read_only property on the connection with a closed-over var
      read_only_opt = opts[:read_only]
      c.stub!( :read_only ).and_return { read_only_opt }
      # gah. stub! has a bug : block doesn't get the arg. have to use should_receive instead
      c.should_receive( :read_only= ).at_least(2).times.and_return { |ro| read_only_opt = ro }

      rc = mock("raw_connection")
      rc.stub!(:connection).and_return(c)
      
      fa = FooAdapter.new( nil, nil, { :adapter => opts[:adapter] } )
      fa.stub!(:raw_connection).and_return(rc)
      fa
    end

    describe "initialize_with_master_slave" do

      it "should alias the _execute method for mysql adapters" do
        a = make_foo_adapter( :adapter=>"mysql" )
        
        a.should_receive( :_execute_without_master_slave )

        a._execute( "select * from foo" )
      end

      it "should not alias the _execute method for non-mysql adapters" do
        a = FooAdapter.new( nil, nil, { :adapter => "not_that_database" } )
        a.should_not_receive( :_execute_without_master_slave )
        a._execute( "select * from foo" )
      end
    end

    describe "_execute_with_master_slave" do

      it "should temporarily set connection read-only if !master_only && auto-commit && select" do
        a = make_foo_adapter( :adapter => "mysql",
                              :master_only => false,
                              :auto_commit => true,
                              :select? => true )

        a.should_receive( :_execute_without_master_slave ).and_return do |sql,name|
          a.raw_connection.connection.read_only.should ==(true)
        end

        a._execute( "select * from foo" )
        a.raw_connection.connection.read_only.should ==(false)
      end

      it "should not set connection read-only if not auto-commit" do
        a = make_foo_adapter( :adapter => "mysql",
                              :auto_commit => false )

        a.should_receive( :_execute_without_master_slave ).and_return do |sql,name|
          a.raw_connection.connection.read_only.should ==(false)
        end

        a._execute( "select * from foo" )
        a.raw_connection.connection.read_only.should ==(false)
      end

      it "should not set connection read-only if master-only" do
        a = make_foo_adapter( :adapter => "mysql",
                              :master_only => true )

        a.should_receive( :_execute_without_master_slave ).and_return do |sql,name|
          a.raw_connection.connection.read_only.should ==(false)
        end

        a._execute( "select * from foo" )
        a.raw_connection.connection.read_only.should ==(false)
      end

      it "should not set connection read-only if ! select" do
        a = make_foo_adapter( :adapter => "mysql",
                              :select? => false )

        a.should_receive( :_execute_without_master_slave ).and_return do |sql,name|
          a.raw_connection.connection.read_only.should ==(false)
        end

        a._execute( "select * from foo" )
        a.raw_connection.connection.read_only.should ==(false)
      end

      it "should restore the read-only status of the connection if an exception is raised" do
        a = make_foo_adapter( :adapter => "mysql" )

        a.should_receive( :_execute_without_master_slave ).and_return do |sql,name|
          a.raw_connection.connection.read_only.should ==(true)
          raise "bang"
        end

        lambda { 
          a._execute( "select * from foo" )
        }.should raise_error
        
        a.raw_connection.connection.read_only.should ==(false)
      end
    end
  end

end
