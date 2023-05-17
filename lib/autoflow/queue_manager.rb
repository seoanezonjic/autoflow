require 'json'
class QueueManager

	def initialize(exec_folder, options, commands, persist_variables)
		@exec_folder = exec_folder
		@commands = commands
		@persist_variables = persist_variables
		@verbose = options[:verbose]
		@show_submit = options[:show_submit_command]
		@job_identifier = options[:identifier]
		@files = {}
		@remote = options[:remote]
		@ssh = options[:ssh]
		@write_sh = options[:write_sh]
		@external_dependencies = options[:external_dependencies]
		@active_jobs = []
		@extended_logging = options[:extended_logging]
		@comment = options[:comment]
		@sleep_time = options[:sleep_time]
	end

	########################################################################################
	## SELECT AND PREPARE MANAGER
	########################################################################################

	def self.descendants
		ObjectSpace.each_object(Class).select { |klass| klass < self }
	end

	def self.select_queue_manager(exec_folder, options, jobs, persist_variables)
		path_managers = File.join(File.dirname(__FILE__),'queue_managers')
		Dir.glob(path_managers+'/*').each do |manager|
			require manager
		end
		if options[:batch]
			queue_manager = BashManager
		else
			queue_manager = select_manager(options)
		end
		warn("Selected queue manager: #{queue_manager}")
		return queue_manager.new(exec_folder, options, jobs, persist_variables)
	end

	def self.select_manager(options)
		queue_manager = nil
		priority = 0
		descendants.each do |descendant|
			if descendant.available?(options) && priority <= descendant.priority
				queue_manager = descendant
				priority = descendant.priority
			end
		end
		return queue_manager
	end

	########################################################################################
	## EXECUTING WORKFLOW WITH MANAGER
	########################################################################################

	def exec
		create_folder(@exec_folder)
		make_environment_file if !@persist_variables.empty?
		create_file('versions', @exec_folder)
		write_file('versions',"autoflow\t#{Autoflow::VERSION}")
		close_file('versions')
		create_file('index_execution', @exec_folder)
		launch_all_jobs
		close_file('index_execution')
	end

	def init_log #TODO adapt to remote execution
		log_path = [@exec_folder, '.wf_log'].join('/') #Join must assume linux systems so File.join canot be used for windows hosts
		log = parse_log(log_path) #TODO modify to folder
		job_relations_with_folders = get_relations_and_folders
		if @write_sh
			create_file('wf.json', @exec_folder)
			write_file('wf.json', job_relations_with_folders.to_json)
			close_file('wf.json')
		end
  		@active_jobs.each do |task|
  			query = log[task]
  			if query.nil?
				log[task] = {'set' => [Time.now.to_i]}
  			else
				log[task]['set'] << Time.now.to_i
  			end
		end
		write_log(log, log_path, job_relations_with_folders)
	end

	def get_relations_and_folders
		relations = {}
		@commands.each do |name, job|
			relations[name] = [job.attrib[:exec_folder], job.dependencies]
		end
		return relations
	end

	def launch_all_jobs
		buffered_jobs = []
		sorted_jobs = sort_jobs_by_dependencies
		sorted_jobs.each do |name, job|
			@active_jobs << job.name if !job.attrib[:done]
		end
		init_log
		sorted_jobs.each do |name, job|
			write_file('index_execution', "#{name}\t#{job.attrib[:exec_folder]}") 
			sleep(@sleep_time) if @sleep_time > 0
			if job.attrib[:done]
				next
			else
				rm_done_dependencies(job)
			end	
			buffered_jobs = launch_job_in_folder(job, name, buffered_jobs)
		end
	end

	def sort_jobs_by_dependencies # We need job ids from queue system so we ask for each job and we give the previous queue system ids as dependencies if necessary
		ar_jobs = @commands.to_a
		sorted_jobs = []
		jobs_without_dep = ar_jobs.select{|job| job.last.dependencies.empty?}
		sorted_jobs.concat(jobs_without_dep)
		while ar_jobs.length != sorted_jobs.length
			ids = sorted_jobs.map{|job| job.first}
			ar_jobs.each do |job|
				if !sorted_jobs.include?(job) 
					deps = job.last.dependencies - ids
					sorted_jobs << job if deps.empty?
				end
			end
		end
		return sorted_jobs
	end

	def rm_done_dependencies(job)
		remove=[]
		job.dependencies.each do |dependency|			
			remove << dependency if @commands[dependency].attrib[:done]
		end
		remove.each do |rm|
			job.dependencies.delete(rm)
		end
	end

	def launch_job_in_folder(job, id, buffered_jobs)
		create_folder(job.attrib[:exec_folder])
		if !job.attrib[:buffer]  # Launch with queue_system the job and all buffered jobs
			launch2queue_system(job, id, buffered_jobs)
			buffered_jobs = []#Clean buffer
		else # Buffer job
			buffered_jobs << [id, job]
		end
		return buffered_jobs	
	end


	def launch2queue_system(job, id, buffered_jobs)
		sh_name = job.name+'.sh'
		if @write_sh
			# Write sh file
			#--------------------------------
			create_file(sh_name, job.attrib[:exec_folder])
			write_file(sh_name, '#!/usr/bin/env bash')
			write_file(sh_name, '##JOB_GROUP_ID='+@job_identifier)
			write_header(id, job, sh_name)
		end

		#Get dependencies
		#------------------------------------
		ar_dependencies = get_dependencies(job, id)
		buffered_jobs.each do |id_buff_job, buff_job|
			ar_dependencies += get_dependencies(buff_job, id_buff_job)
			if @write_sh
				write_job(buff_job, sh_name)
				buff_job.attrib[:exec_folder] = job.attrib[:exec_folder]
			end
		end
		ar_dependencies.uniq!

		if @write_sh
			#Write sh body
			#--------------------------------
			write_file(sh_name, 'hostname')
			log_file_path = [@exec_folder, '.wf_log', File.basename(job.attrib[:exec_folder])].join('/')
			write_file(sh_name, "flow_logger -e #{log_file_path} -s #{job.name}")
			write_file(sh_name, "source #{File.join(@exec_folder, 'env_file')}") if !@persist_variables.empty?
			write_job(job, sh_name)
			write_file(sh_name, "flow_logger -e #{log_file_path} -f #{job.name}")
			write_file(sh_name, "echo 'General time'")
			write_file(sh_name, "times")
			close_file(sh_name, 0755)
		end

		#Submit node
		#-----------------------------------
		if !@verbose
			queue_id = submit_job(job, ar_dependencies)
			job.queue_id = queue_id # Returns id of running tag on queue system 
			asign_queue_id(buffered_jobs, queue_id)
		end
	end

	def make_environment_file
		create_file('env_file', @exec_folder)
		@persist_variables.each do |var, value|
			write_file('env_file', "export #{var}=#{value}")
		end
		close_file('env_file')
	end

	def create_folder(folder_name)
		if @remote
			@ssh.exec!("if ! [ -d  #{folder_name} ]; then mkdir -p #{folder_name}; fi")
		else
			Dir.mkdir(folder_name) if !File.exists?(folder_name)
		end
	end

	def create_file(file_name, path) 
		@files[file_name] = [path, '']
	end

	def write_file(file_name, content)
		@files[file_name].last << content+"\n"
	end

	def close_file(file_name, permissions = nil) #SSH
		path, content = @files.delete(file_name)
		file_path = File.join(path, file_name)
		if @remote
			@ssh.exec!("echo '#{content}' > #{file_path}")
			@ssh.exec!("chmod #{permissions} #{file_path}") if !permissions.nil?
		else
			local_file = File.open(file_path,'w')
			local_file.chmod(permissions) if !permissions.nil?
			local_file.print content
			local_file.close
		end
	end

	def read_file(file_path)
		content = nil
		if @remote
			res = @ssh.exec!("[ ! -f #{file_path} ] && echo 'Autoflow:File Not Found' || cat #{file_path}")
			content = res if !content.include?('Autoflow:File Not Found')
		else
			content = File.open(file_path).read if File.exists?(file_path)
		end
		return content
	end

	def system_call(cmd, path = nil)
		cmd = "cd #{path}; " + cmd if !path.nil?
		if @remote
			call = @ssh.exec!(cmd)
		else
			call = %x[#{cmd}] 
		end
		return call
	end

	def self.system_call(cmd, path = nil, remote = FALSE, ssh = nil)
		cmd = "cd #{path}; " + cmd if !path.nil?
		if remote
			call = ssh.exec!(cmd)
		else
			call = %x[#{cmd}] 
		end
		return call
	end

	def write_job(job, sh_name)	
		write_file(sh_name, job.initialization) if !job.initialization.nil?
		if @comment
			cmd = '#' + job.parameters
		else
			if @extended_logging
				log_command = '/usr/bin/time -o process_data -v '
			else
				log_command = 'time '
			end
			cmd = log_command + job.parameters
		end
		write_file(sh_name, cmd)
	end

	def get_dependencies(job, id = nil)
		ar_dependencies = []
		ar_dependencies += job.dependencies
		ar_dependencies.delete(id) if !id.nil? #Delete autodependency
		return ar_dependencies
	end

	def asign_queue_id(ar_jobs, id)
		ar_jobs.each do |id_job, job|
			job.queue_id=id
		end
	end
	
	def get_queue_system_dependencies(ar_dependencies)
		queue_system_ids=[]
		ar_dependencies.each do |dependency|
			queue_system_ids << @commands[dependency].queue_id
		end
		return queue_system_ids
	end

	def get_all_deps(ar_dependencies)
		final_dep = []
		final_dep.concat(get_queue_system_dependencies(ar_dependencies)) if !ar_dependencies.empty?
		final_dep.concat(@external_dependencies)
		return final_dep
	end

	########################################################################################
	## QUEUE DEPENDANT METHODS
	########################################################################################
	def write_header(id, node, sh)

	end

	def submit_job(job, ar_dependencies)

	end

	def get_queue_system_id(shell_output)

	end

	def self.available?
		return FALSE
	end

	def self.priority
		return -1
	end
end