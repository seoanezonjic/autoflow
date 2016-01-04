require 'queue_manager'
class BashManager < QueueManager

	def initialize(exec_folder, options, commands, persist_variables)
		super
		@queued = []
		@last_deps = []
		@path2execution_script = File.join(@exec_folder, 'execution.sh') 
		create_file('execution.sh', @exec_folder)
		write_file('execution.sh', '#! /usr/bin/env bash')
	end

	def launch_all_jobs
		super
		close_file('execution.sh', 0755)
		system_call("#{@path2execution_script} > #{File.join(File.dirname(@path2execution_script),'output')} & ", @exec_folder)
	end

	def write_header(id, node, sh)
		@queued << id # For dependencies purposes
	end

	def submit_job(job, ar_dependencies)
		write_file('execution.sh','')
		if !ar_dependencies.empty? 
			deps = ar_dependencies - @last_deps
			if !deps.empty?
				write_file('execution.sh', 'wait') 
				@last_deps.concat(@queued)
			end
		end
		@last_deps.concat(ar_dependencies)
		@last_deps.uniq!
		write_file('execution.sh', "cd #{job.attrib[:exec_folder]}")
		write_file('execution.sh', "./#{job.name}.sh &")
		return nil
	end

	def get_queue_system_id(shell_output)
		return nil
	end

	def self.available?(options)
		return TRUE
	end

	def self.priority
		return 0 
	end
end