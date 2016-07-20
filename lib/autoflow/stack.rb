require 'program'
class Stack
	
	def initialize(exec_folder,options)
		@exec_folder=exec_folder
		@file_workflow=options[:workflow]
		@cpus=options[:cpus]
		@time=options[:time]
		@memory=options[:memory]
		@commands={}
		@dirs=[]
		@node_type=options[:node_type]
		@exp_cpu=options[:exp_cpu]
		@count_cpu=0
		@use_multinode = options[:use_multinode]
		parse(options[:workflow], options[:retry])
	end
	
	def parse(file_workflow, do_retry)
		count=0
		File.open(file_workflow).each do |line|
			line.chomp!
			if line =~/^#/ || line.empty? #Saltar lineas en blanco y comentarios
				next
			end
			fields=line.split("\t")
			prog_parameters=fields[1].split(' ',2)		
			folder=asign_folder(prog_parameters[0])			
			done=FALSE
			iterated=FALSE
			end_tasks_buffer=TRUE
			if fields[0]=~ /\!/
				folder=@exec_folder
			end
			if fields[0]=~ /-/
				end_tasks_buffer=FALSE
				folder=@exec_folder
			end
			if fields[0]=~ /\+/
				iterated=TRUE
			end
			if fields[0]=~ /\%/
				done=TRUE
			end
			fields[0].gsub!(/\+|-|\!|\%/,'')# Delete function characters
			#Dependencies
			parameters, dependencies = parse_parameters(prog_parameters[1])
			initialization, init_dependencies = parse_parameters(fields[2])
			all_dependencies= dependencies + init_dependencies
			command_line=prog_parameters[0]+' '+parameters
			add_program(fields[0], prog_parameters[0], command_line, initialization, iterated, folder, all_dependencies, done, end_tasks_buffer)
		end
	end


	def add_program(stage_id, name, parameters, initialization, iterated, exec_folder_program, dependencies, done, end_tasks_buffer)
		task=Program.new(name, parameters, initialization, iterated, exec_folder_program, dependencies, done, end_tasks_buffer)
		@commands[stage_id]=task
		return task
	end

	def parse_parameters(orig_param)
		dependencies=[]
		if !orig_param.nil?
			@commands.keys.each do |stage_id|
				if orig_param.include?(stage_id)
					dependencies << stage_id
				end
				orig_param.gsub!(stage_id, @commands[stage_id].exec_folder_program) #Change id task to folder task
			end
			if orig_param.include?(')')
				raise 'Missed dependency on: ' + orig_param
			end
		end
		return orig_param, dependencies
	end

	def inspect
		@commands.each do |id, task|
			puts "#{id}  |  #{task.inspect}" 
		end
	end

	def exec
		buffered_tasks=[]
		index_execution = File.open('index_execution','w')							
		@commands.each do |id, task|
			index_execution.puts "#{id}\t#{task.exec_folder_program}"
			if task.done
				next
			else
				rm_done_dependencies(task)
			end
			if !File.exists?(task.exec_folder_program)
				Dir.mkdir(task.exec_folder_program)
			end
			# Task execution within folder attibute
			Dir.chdir(task.exec_folder_program) do
				if !task.end_tasks_buffer # Buffer task if cmd
					buffered_tasks << [id, task]
				else # Launch with queue_system tasks and all buffered
					launch_queue_system(id, task, buffered_tasks)
					buffered_tasks=[]#Clean buffer
				end
			end
		end
		index_execution.close
	end

	def rm_done_dependencies(task)
		remove=[]
		task.dependencies.each do |dependency|
			if @commands[dependency].done
				remove << dependency
			end
		end
		remove.each do |rm|
			task.dependencies.delete(rm)
		end
	end

	def launch_queue_system(id, task, buffered_tasks)
		# Write sh file
		#--------------------------------
		log_folder=File.join(@exec_folder,'log')
		sh=File.open(task.name+'.sh','w')
		used_cpu=1
		if !task.monocpu
			if @exp_cpu == 0
				used_cpu=@cpus
			else
				if @exp_cpu**(@count_cpu) < @cpus
					@count_cpu +=1
				end
				used_cpu = @exp_cpu**@count_cpu
			end 
		end
		contraint = nil
		if !@node_type.nil?
			constraint ='#SBATCH --constraint='+@node_type 
		end	
		sh.puts 	'#!/usr/bin/env bash',
				'# The name to show in queue lists for this job:',
				"##SBATCH -J #{task.name}.sh",
				'# Number of desired cpus:'
		if @use_multinode == 0
			sh.puts "#SBATCH --cpus=#{used_cpu}"
		else
			sh.puts "#SBATCH --tasks=#{used_cpu}",
					"#SBATCH --nodes=#{@use_multinode}",
					"srun hostname -s > workers"
		end
		sh.puts	'# Amount of RAM needed for this job:',
				"#SBATCH --mem=#{@memory}",
				'# The time the job will be running:',
				"#SBATCH --time=#{@time}",
				'# To use GPUs you have to request them:',
				'##SBATCH --gres=gpu:1',
				"#{constraint}",
				'# Set output and error files',
				'#SBATCH --error=job.%J.err',
				'#SBATCH --output=job.%J.out',
				'# MAKE AN ARRAY JOB, SLURM_ARRAYID will take values from 1 to 100',
				'##SARRAY --range=1-100',
				'# To load some software (you can show the list with \'module avail\'):',
				'# module load software',
				'hostname',
				"echo STARTED  #{id.gsub(')','')}  #{task.name} >> #{log_folder}",
				"date >> #{log_folder}"
		ar_dependencies=[]
		ar_dependencies+=task.dependencies
		ar_dependencies.delete(id) #Delete autodependency

		buffered_tasks.each do |id_buff_task,buff_task|
			write_task(buff_task,sh)
			ar_dependencies+=buff_task.dependencies
			ar_dependencies.delete(id_buff_task) #Delete autodependency
			buff_task.exec_folder_program=task.exec_folder_program
		end
		
		write_task(task,sh)
		sh.puts	"echo FINISHED  #{id.gsub(')','')}  #{task.name} >> #{log_folder}",
				"date >> #{log_folder}"
		sh.close

		#Submitt task
		#-----------------------------------
		dependencies=nil
		if !ar_dependencies.empty?
			ar_dependencies.uniq!
			queue_system_id_dependencies=get_queue_system_dependencies(ar_dependencies)
			dependencies='--dependency=afterok:'+queue_system_id_dependencies.join(':')
		end
		cmd ="sbatch #{dependencies} #{task.name}.sh"
		shell_output=%x[#{cmd}]
		shell_output.chomp!
		fields=shell_output.split(' ')
		task.queue_id=fields[3] # Returns id of running task on queue system 
		asign_queue_id(buffered_tasks,fields[3])
	end

	def write_task(task, sh)
		if !task.initialization.nil?
			sh.puts task.initialization
		end
		sh.print 'time '
		used_cpu=1.to_s
		if !task.monocpu
			if @use_multinode == 0
				used_cpu=@cpus.to_s
			else
				used_cpu = 'workers'
			end
		end
		task.parameters.gsub!('[cpu]',used_cpu) #Use asigned cpus
		sh.puts task.parameters
	end

	def asign_folder(program_name)			
		folder=File.join(@exec_folder,program_name)
		count=0
		folder_bk=folder
		while @dirs.include?(folder_bk)
			folder_bk=folder+'_'+count.to_s
			count+=1
		end
		@dirs << folder_bk
		return folder_bk
	end
	
	def get_dependencies(task)
		dependencies=[]
		task.dependencies.each do |dep|
			dependencies << @commands[dep].queue_id
		end
		return dependencies
	end

	def asign_queue_id(ar_tasks,id)
		ar_tasks.each do |id_ar_task, ar_task|
			ar_task.queue_id=id
		end
	end
	
	def get_queue_system_dependencies(ar_dependencies)
		queue_system_ids=[]
		ar_dependencies.each do |dependency|
			puts dependency
			queue_system_ids << @commands[dependency].queue_id
		end
		return queue_system_ids
	end
end
