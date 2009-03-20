if RUBY_PLATFORM =~ /java/ && ENV['NODB'].nil?
  require 'active_record_jdbc_mysql_master_slave'
end
