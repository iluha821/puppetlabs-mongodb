#
# Author: François Charlier <francois.charlier@enovance.com>
#

require File.expand_path(File.join(File.dirname(__FILE__), '..', 'mongodb'))
Puppet::Type.type(:mongodb_replset).provide(:mongo, :parent => Puppet::Provider::Mongodb) do

  desc "Manage hosts members for a replicaset."

  confine :true =>
    begin
      require 'json'
      true
    rescue LoadError
      false
    end

  mk_resource_methods

  def initialize(resource={})
    Puppet.debug "Initialize"
    super(resource)
    @property_flush = {}
  end

  def members=(hosts)
    @property_flush[:members] = hosts
  end

  def self.instances
    Puppet.debug "instances"
    begin
      instance = get_replset_properties
    rescue => e
      Puppet.debug "Got an exception: #{e}"
    end

    if instance
      # There can only be one replset per node
      [new(instance)]
    else
      []
    end
  end

  def self.prefetch(resources)
    Puppet.debug "Prefetch"
    instances.each do |prov|
      if resource = resources[prov.name]
        resource.provider = prov
      end
    end
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def create
    @property_flush[:ensure] = :present
    @property_flush[:members] = resource.should(:members)
  end

  def destroy
    @property_flush[:ensure] = :absent
  end

  def flush
    Puppet.debug "flush"
    set_members
    @property_hash = self.class.get_replset_properties
  end

  private

  def db_ismaster(host)
    mongo_command('db.isMaster()', host)
  end

  def rs_initiate(conf, master)
    Puppet.debug "rs_initiate auth_enabled #{auth_enabled}"
    if auth_enabled
      return mongo_command("rs.initiate(#{conf})", initialize_host)
    else
      return mongo_command("rs.initiate(#{conf})", master)
    end
  end

  def rs_status(host)
    mongo_command('rs.status()', host)
  end

  def rs_add(host, master, priority=nil, hidden=nil, votes=nil)
    priority_conf = ""
    if priority
      priority_conf = ", priority: #{priority.to_i} "
    end

    hidden_conf = ""
    if hidden
      hidden_conf = ", hidden: #{hidden} "
    end

    votes_conf = ""
    if votes
      votes_conf = ", votes: #{votes.to_i} "
    end

    mongo_command("rs.add({host: '#{host}' #{priority_conf} #{hidden_conf} #{votes_conf} })", master)
  end

  def rs_remove(host, master)
    mongo_command("rs.remove('#{host}')", master)
  end

  def rs_arbiter
    @resource[:arbiter]
  end

  def rs_add_arbiter(host, master)
    mongo_command("rs.addArb('#{host}')", master)
  end

  def auth_enabled
    self.class.auth_enabled
  end

  def initialize_host
    @resource[:initialize_host]
  end

  def master_host(hosts)
    hosts.each do |host|
      status = db_ismaster(host)
      if status.has_key?('primary')
        return status['primary']
      end
    end
    false
  end

  def self.get_replset_properties
    conn_string = get_conn_string
    print "get_replset_properties #{conn_string}"
    output = mongo_command('rs.conf()', conn_string)
    if output['members']
      members = output['members'].collect do |val|
        val['host']
      end
      props = {
        :name     => output['_id'],
        :ensure   => :present,
        :members  => members,
        :provider => :mongo,
      }
    else
      props = nil
    end
    Puppet.debug("MongoDB replset properties: #{props.inspect}")
    props
  end

  def alive_members(hosts)
    alive = []
    hosts.select do |host|
      begin
        Puppet.debug "Checking replicaset member #{host} ..."
        status = rs_status(host)
        Puppet.debug "Status: #{status}"
        if status.has_key?('errmsg') and status['errmsg'] == 'not running with --replSet'
          raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is not supposed to be part of a replicaset."
        end

        if auth_enabled and status.has_key?('errmsg') and (status['errmsg'].include? "unauthorized" or status['errmsg'].include? "not authorized")
          Puppet.warning "Host #{host} is available, but you are unauthorized because of authentication is enabled: #{auth_enabled}"
          alive.push(host)
        end

        if status.has_key?('set')
          if status['set'] != self.name
            raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is already part of another replicaset."
          end

          # This node is alive and supposed to be a member of our set
          Puppet.debug "Host #{host} is available for replset #{status['set']}"
          alive.push(host)
        elsif status.has_key?('info')
          Puppet.debug "Host #{host} is alive but unconfigured: #{status['info']}"
          alive.push(host)
        end
      rescue Puppet::ExecutionFailure
        Puppet.warning "Can't connect to replicaset member #{host}."
      end
    end
    return alive
  end

  def extract_hosts(hosts_config)
    return hosts_config[0].keys
  end

  def priority(hosts_conf, host)
    Puppet.debug "Host conf #{hosts_conf} host #{host}"
    if hosts_conf[0][host]
      return hosts_conf[0][host]['priority']
    else
      return nil
    end
  end

  def hidden(hosts_conf, host)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['hidden']
    else
      return nil
    end
  end

  def votes(hosts_conf, host)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['votes']
    else
      return nil
    end
  end

  def ssl_on(host)
    return self.class.ssl_on(host, @property_flush[:members])
  end

  def self.ssl_on(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['ssl']
    else
      return false
    end
  end


  def sslCAFile(host)
    return self.class.sslCAFile(host, @property_flush[:members])
  end

  def self.sslCAFile(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['sslCAFile']
    else
      return nil
    end
  end


  def sslPEMKeyFile(host)
    return self.class.sslPEMKeyFile(host, @property_flush[:members])
  end

  def self.sslPEMKeyFile(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['sslPEMKeyFile']
    else
      return nil
    end
  end

  def authenticationDatabase(host)
    return self.class.authenticationDatabase(host, @property_flush[:members])
  end

  def self.authenticationDatabase(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['authenticationDatabase']
    else
      return nil
    end
  end

  def authenticationMechanism(host)
    return self.class.authenticationMechanism(host, @property_flush[:members])
  end

  def self.authenticationMechanism(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['authenticationMechanism']
    else
      return nil
    end
  end

  def username(host)
    return self.class.username(host, @property_flush[:members])
  end

  def self.username(host, hosts_conf)
    if hosts_conf[0][host]
      return hosts_conf[0][host]['username']
    else
      return nil
    end
  end   

  def set_members
    Puppet.debug "Set_members"
    if @property_flush[:ensure] == :absent
      # TODO: I don't know how to remove a node from a replset; unimplemented
      #Puppet.debug "Removing all members from replset #{self.name}"
      #@property_hash[:members].collect do |member|
      #  rs_remove(member, master_host(@property_hash[:members]))
      #end
      return
    end

    if ! @property_flush[:members].empty?
      extracted_hosts=extract_hosts(@property_flush[:members])
      # Find the alive members so we don't try to add dead members to the replset
      alive_hosts = alive_members(extracted_hosts)
      dead_hosts  = extracted_hosts - alive_hosts
      Puppet.debug "Alive members: #{alive_hosts.inspect}"
      Puppet.debug "Dead members: #{dead_hosts.inspect}" unless dead_hosts.empty?
      raise Puppet::Error, "Can't connect to any member of replicaset #{self.name}." if alive_hosts.empty?
    else
      alive_hosts = extract_hosts(@property_flush[:members])
    end

    if @property_flush[:ensure] == :present and @property_hash[:ensure] != :present and !master_host(alive_hosts)
      Puppet.debug "Initializing the replset #{self.name}"

      # Create a replset configuration
      hostconf = alive_hosts.each_with_index.map do |host,id|
        Puppet.debug "Host ID #{host} #{id}"

        arbiter_conf = ""
        if rs_arbiter == host
          arbiter_conf = ", arbiterOnly: \"true\""
        end
        
        priority_conf = ""
        priority_val = priority(@property_flush[:members], host)
        if priority_val
          priority_conf = ", priority: #{priority_val.to_i} "
        end

        hidden_conf = ""
        hidden_val = hidden(@property_flush[:members], host)
        if hidden_val
          hidden_conf = ", hidden: #{hidden_val} "
        end

        votes_conf = ""
        votes_val = votes(@property_flush[:members], host)
        if votes_val
          votes_conf = ", votes: #{votes_val.to_i} "
        end

        "{ _id: #{id}, host: \"#{host}\"#{arbiter_conf} #{priority_conf} #{hidden_conf} #{votes_conf} }"
      end.join(',')

      conf = "{ _id: \"#{self.name}\", members: [ #{hostconf} ] }"
      Puppet.debug "mongo conf #{conf}"
      
      # Set replset members with the first host as the master
      output = rs_initiate(conf, alive_hosts[0])
      if output['ok'] == 0
        raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: #{output['errmsg']}"
      end

      # Check that the replicaset has finished initialization
      retry_limit = 10
      retry_sleep = 3

      retry_limit.times do |n|
        begin
          if db_ismaster(alive_hosts[0])['ismaster']
            Puppet.debug 'Replica set initialization has successfully ended'
            return
          else
            Puppet.debug "Wainting for replica initialization. Retry: #{n}"
            sleep retry_sleep
            next
          end
        end
      end
      raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: host #{alive_hosts[0]} didn't become master"

    else
      # Add members to an existing replset
      Puppet.debug "Adding member to existing replset #{self.name}"
      if master = master_host(alive_hosts)
        master_data = db_ismaster(master)
        current_hosts = master_data['hosts']
        current_hosts = current_hosts + master_data['arbiters'] if master_data.has_key?('arbiters')
        Puppet.debug "Current Hosts are: #{current_hosts.inspect}"
        newhosts = alive_hosts - current_hosts
        Puppet.debug "New Hosts are: #{newhosts.inspect}"
        newhosts.each do |host|
          output = {}
          if rs_arbiter == host
            output = rs_add_arbiter(host, master)
          else
            output = rs_add(host, master, priority=priority(@property_flush[:members], host), hidden=hidden(@property_flush[:members], host), votes=votes(@property_flush[:members], host))
          end
          if output['ok'] == 0
            raise Puppet::Error, "rs.add() failed to add host to replicaset #{self.name}: #{output['errmsg']}"
          end
        end
      else
        raise Puppet::Error, "Can't find master host for replicaset #{self.name}."
      end
    end
  end

  def mongo_command(command, host, retries=4)
    Puppet.debug "mongo_command #{command}, host #{host}, retries #{retries}"
    ssl = ssl_on(host)
    sslCAFile = sslCAFile(host)
    sslPEMKeyFile = sslPEMKeyFile(host)
    authenticationDatabase = authenticationDatabase(host)
    username = username(host)
    authenticationMechanism = authenticationMechanism(host)

    self.class.mongo_command(command, host, retries, ssl, sslCAFile , sslPEMKeyFile, authenticationDatabase, authenticationMechanism, username)
  end

  def self.mongo_command(command, host=nil, retries=4, ssl=false, sslCAFile=nil, sslPEMKeyFile=nil, authenticationDatabase=nil, authenticationMechanism=nil, username=nil)
    begin
      Puppet.debug "Mongo command #{command}"
      Puppet.debug "ssl: #{ssl}"
      if ssl
        begin
          output = mongo_eval("printjson(#{command})", 'admin', retries, host, ssl, sslCAFile , sslPEMKeyFile, authenticationDatabase, authenticationMechanism, username)
        rescue => e
          Puppet.debug "mongo command - non-ssl fallback"
          output = mongo_eval("printjson(#{command})", 'admin', retries, host)
        end
      else
        output = mongo_eval("printjson(#{command})", 'admin', retries, host)
      end
    rescue Puppet::ExecutionFailure => e
      Puppet.debug "Got an exception: #{e}"
      raise
    end

    # Dirty hack to remove JavaScript objects
    output.gsub!(/ISODate\((.+?)\)/, '\1 ')
    output.gsub!(/Timestamp\((.+?)\)/, '[\1]')

    #Hack to avoid non-json empty sets
    output = "{}" if output == "null\n"

    # Parse the JSON output and return
    JSON.parse(output)
  end
end
