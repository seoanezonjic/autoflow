class Batch
	attr_accessor :name, :iterator, :dependencies, :init, :main_command, :attrib, :id, :jobs, :parent

	@@all_batch = {}
	@@jobs_names = []
	@@batch_iterator_relations = {}
	@@nested_iteration_relations = {}
	@@general_computation_attrib = {
		:cpu => nil, 
		:mem => nil, 
		:time => nil,
		:node => nil,
		:multinode => nil,
		:ntask => nil
	}
 
	def self.set_general_attrib(attrib_hash)
		@@general_computation_attrib = attrib_hash
	end


	def initialize(tag, init, main_command, id, exec_folder)
		@regex_deps = nil
		replace_regexp(tag, init, main_command)
		@name = nil
		@id = id
		@iterator = [nil] 
		@parent = nil
		@dependencies = [] # [batch_name, dependency_type, keyword2replace]
			# nil => There isn't dependecies, 
			# 'simple' => One job needs a previous job, 
			# '1to1' => A job in a batch needs another job in other batch, 
			# '*to1' => A job need a previous full batch of jobs
			# 'local' => A simple job needs one job of a batch
		@initialization = init
		@main_command = main_command
		@attrib = {
			:done => false,
			:folder => true,
			:buffer => false,
			:exec_folder => exec_folder,
			:cpu_asign => nil # number, list or mono
		}.merge(@@general_computation_attrib)
		get_name_and_iterators_and_modifiers(tag)
		set_execution_attrib
		set_cpu
		@jobs = []
		@@all_batch[@name] = self
	end

	def replace_regexp(tag, init, main_command)
		scan_JobRegExp_tag(tag) if tag.include?('JobRegExp:') 
		[init, main_command].each do |intructions|
			if intructions.class.to_s == 'String'
				while intructions.include?('!JobRegExp:')
					scan_JobRegExp(intructions)
				end
			end
		end
		#puts main_command.inspect
	end

	def scan_JobRegExp(command)
		data = /!JobRegExp:([^ \n]+):([^ \n]+)!([^ \n]+)/.match(command) # *to1 with regexp
		#data[0] => reference string (command), data[1] => batch_pattern, data[2] => iterator_pattern, data[3] => adyacent string to regexp as regexp/file_name
		job_names = get_dependencies_by_regexp(data[1], data[2])
		@regex_deps = 'command' if job_names.length > 0
		new_string = job_names.map{|jn| jn + ')' + data[3] }.join(' ')
		command.gsub!(data[0], new_string)
		#puts command.inspect
	end

	def scan_JobRegExp_tag(tag)
		data = /JobRegExp:([^ \n]+):([^;\] \n]+)/.match(tag) # 1to1 with regexp
		#data[0] => reference string (command), data[1] => batch_pattern, data[2] => iterator_pattern
		job_names = get_dependencies_by_regexp(data[1], data[2])
		if job_names.length > 0
			@regex_deps = 'tag' 
		end
		new_string = job_names.map{|jn| jn + ')'}.join(';')
		tag.gsub!(data[0], new_string)
	end

	def get_dependencies_by_regexp(batch_pattern, iterator_pattern)
		selected_batches = @@all_batch.keys.select{|cmd_name| cmd_name =~ /#{batch_pattern}/}
		job_names = []
		selected_batches.each do |batch_name|
			iterators = @@all_batch[batch_name].iterator
			iterators = iterators.map{|it| it.gsub(/&|\!|\%|\)/,'')} if !iterators.first.nil?
			if iterator_pattern != '-'
				next if iterators.first.nil?
				iterators = iterators.select{|iter| iter =~ /#{iterator_pattern}/} 
			end
			if !iterators.empty?
				if iterators.first.nil? 
					job_names << batch_name
				else
					iterators.each do |iter|
						job_names << batch_name+iter
					end
				end
			end
		end
		return job_names
	end

	def has_jobs?
		res = !@jobs.empty?
		return res
	end

	def asign_child_batch
		batches = []
		if @main_command.class.to_s == 'Array'
			@main_command.each do |id|
				batch = get_batch(id)
				batch.parent = @name
				batches << batch
			end
			@main_command = batches
		end
	end

	def get_batch(id)
		selected_batch = nil
		@@all_batch.each do |name, batch|
			if batch.id == id
				selected_batch = batch  
				break
			end
		end
		return selected_batch
	end

	def get_name_and_iterators_and_modifiers(tag)
		tag =~ /(^.+)\[([^\]]+)\]\)/ # iterative node
		name = $1
		if $1.nil? # Non iterative node (simple node)
			tag =~ /(^.+)\)/
			name = $1
		end
		@name , @attrib[:done], @attrib[:folder], @attrib[:buffer] = check_execution_modifiers(name)
		if !$2.nil?
			@iterator = []
			#$2.split(';').map{|iter| iter.gsub(')','')}.each do |interval|
			$2.split(';').each do |interval|
				if interval.include?('-')
					limits = interval.split('-')
					@iterator.concat((limits.first..limits.last).to_a.map{|n| n.to_s})
				else
					@iterator << interval
				end
			end
		end
		@@batch_iterator_relations[@name] = @iterator
	end

	def check_execution_modifiers(name, iter_type = false) #The last paremeter iused to indicate tha name is a iterator not an orignal node name
		done = false
		folder = true
		buffer = false
		done = true if name.include?('%')
		folder = false if name.include?('!')
		buffer = true if name.include?('&')
		if !iter_type
			name.gsub!(/&|\!|\%|\)/,'')# Delete function characters
		else
			name.gsub!(/&|\!|\%/,'')# Delete function characters
		end
		return name, done, folder, buffer
	end

	def set_execution_attrib
		@initialization = scan_resources(@initialization) if !@initialization.nil?
		@main_command = scan_resources(@main_command) if @main_command.class.to_s == 'String'
	end

	def scan_resources(command)
		resources_line = nil
		command.each_line do |line|
			if line.include?('resources:')
				line = line.chomp
				resources_line = line
				fields = line.split(' ')
				fields.each_with_index do |field, index|
					if field == '-c'
						@attrib[:cpu] = fields[index+1].to_i
					elsif field == '-m'
						@attrib[:mem] = fields[index+1]
					elsif field == '-n'
						@attrib[:node] = fields[index+1]
					elsif field == '-t'
						@attrib[:time] = fields[index+1]
					elsif field == '-u'
						@attrib[:multinode] = fields[index+1].to_i
					end	
				end
				if fields.include?('-s')
					@attrib[:ntask] = true
				else
					@attrib[:ntask] = false
				end
			end
		end
		command.gsub!(resources_line, '') if !resources_line.nil?
		return command
	end

	def set_cpu
		@initialization = scan_cpu(@initialization) if !@initialization.nil?
		@main_command = scan_cpu(@main_command) if @main_command.class.to_s == 'String'
		@attrib[:cpu] = 1 if @attrib[:cpu_asign] == 'mono'
	end

	def scan_cpu(command)
		if command.include?('[cpu]')
			command.gsub!('[cpu]', @attrib[:cpu].to_s)
			@attrib[:cpu_asign] = 'number'
		elsif command.include?('[lcpu]')
			command.gsub!('[lcpu]', 'workers')
			@attrib[:cpu_asign] = 'list'
		elsif @attrib[:cpu_asign].nil?
			@attrib[:cpu_asign] = 'mono'
		end				
		return command
	end


	def duplicate_job(tmp_j, sufix_name = '')
		new_job = tmp_j.clone
		new_job.name = tmp_j.name+'_'+sufix_name
		new_job.attrib = tmp_j.attrib.clone
		new_job.dependencies = tmp_j.dependencies.clone
		new_job.initialization = tmp_j.initialization.clone
		new_job.parameters = tmp_j.parameters.clone
		return new_job
	end

	def delete_jobs(jobs2delete, job_array)
		jobs2delete.uniq!
		jobs2delete.sort{|s1, s2| s2 <=> s1}.each do |index|
			job_array.delete_at(index)
		end
		return job_array
	end

	def get_jobs
		jobs = []
		#@@batch_iterator_relations[@name] = @iterator #??????
		if @main_command.class.to_s == 'Array' # There are nested batchs
			temp_jobs = []
			@main_command.each do |batch|
				temp_jobs.concat(batch.get_jobs)
			end
			jobs2delete = []
			@iterator.each_with_index do |iter, i|
				temp_jobs.each_with_index do |tmp_j, tmp_i|
					new_job = duplicate_job(tmp_j, iter)
					check_dependencies(new_job, iter, temp_jobs)
					parse_iter(iter, @name, new_job)
					add_nested_iteration_relation(tmp_j, new_job)
					@@jobs_names << new_job.name
					jobs << new_job
					@jobs << new_job
					jobs2delete << tmp_i
				end
			end
			temp_jobs = delete_jobs(jobs2delete, temp_jobs) #Remove temporal jobs
		else
			check_regex_dependencies
			@iterator.each_with_index do |iter, num|
				job_attrib = @attrib.dup
				if !iter.nil?
					iter, done, job_attrib[:folder], job_attrib[:buffer] = check_execution_modifiers(iter, true)
					job_attrib[:done] = done if !@attrib[:done] # To keep attrib priority in batch on job
				end
				name = "#{@name}#{iter}"
				job_dependencies = []
				batch_deps = @dependencies.length
				initialization = replace_dependencies(@initialization, job_dependencies, iter, num)
				parameters = replace_dependencies(@main_command, job_dependencies, iter, num)
				@dependencies.pop(@dependencies.length - batch_deps) # Clean temporal dependencies by regexp
				job = Program.new(name, initialization, parameters, job_dependencies, job_attrib)
				job.batch = @name
				@@jobs_names << job.name
				jobs << job
				@jobs << job 
			end
		end
		return jobs
	end

	def add_nested_iteration_relation(tmp_j, new_job)
		query = @@nested_iteration_relations[tmp_j.name]
		if query.nil?
			@@nested_iteration_relations[tmp_j.name] = [new_job.name]
		else
			query << new_job.name
		end
	end

	def check_regex_dependencies
		if @regex_deps == 'tag'
			new_job_names = []
			@iterator.each do |iter|
				new_names = find_job_names(iter.gsub(')', ''))
				new_job_names.concat(new_names) 
			end
			@iterator = new_job_names.map{|nj| nj + ')'} if !new_job_names.empty?
		elsif @regex_deps == 'command'
			[@initialization, @main_command].each do |command|
				patterns = command.scan(/([^\s)]+)\)([^\s]*)/)
				if !patterns.empty?
					patterns.each do |putative_job, sufix|
						job_names = find_job_names(putative_job)
						if !job_names.empty?
							new_string = job_names.map{|jn| "#{jn})#{sufix}"}.join(' ')
							old_string = "#{putative_job})#{sufix}"
							command.gsub!(old_string, new_string)
						end
					end
				end
			end
		end
	end

	def find_job_names(name)
		final_names = []
		intermediary_names = @@nested_iteration_relations[name]
		if !intermediary_names.nil?
			while !intermediary_names.empty?
				final_names = intermediary_names
				i_names = []
				intermediary_names.each do |i_n|
					query = @@nested_iteration_relations[i_n]
					i_names.concat(query) if !query.nil?
				end
				if !i_names.empty?
					intermediary_names = i_names
				else 
					break
				end
			end
		end
		return final_names
	end

	#tmp_j => job to set dependencies in iteration
	#iter => sufix of current iteration
	#jobs => array of jobs which has the job dependency	
	def check_dependencies(tmp_j, iter, jobs)
		jobs_names = jobs.map{|job| job.name}
		deps = {}
		tmp_j.dependencies.each_with_index do |dep, i|		
			deps[dep] = i if jobs_names.include?(dep)
		end
		deps.each do |name, index|
			dep = name+'_'+iter
			tmp_j.initialization.gsub!(name+')', dep+')')
			tmp_j.parameters.gsub!(name+')', dep+')')
			tmp_j.dependencies[index] = dep 
		end
	end

	def parse_iter(iter, name, job)
		job.parameters = set_iter(name, iter, job.parameters)
		job.initialization = set_iter(name, iter, job.initialization)
	end

	def set_iter(name, iter, string)
		string = string.gsub(name+'(+)', iter)
		return string
	end

	def handle_dependencies(dinamic_variables)
		[@initialization, @main_command].each do |instructions|
			if instructions.class.to_s == 'String'
				#scan_dependencies(instructions) # NOT NECESSARY? REMOVED BY COLLISION CON REGEX SYSTEM. THE DINAMYC VARIABLES ARE NO USED
				dinamic_variables.concat(collect_dinamic_variables(instructions))
				@dependencies.concat(check_dependencies_with_DinVar(instructions, dinamic_variables))
			end
		end
		return dinamic_variables
	end

	def scan_dependencies(command)
		if !command.nil?# When command is the initialize, sometimes can be undefined
			matched_regions = []
			batches = []
			@@all_batch.each do |k , val| #sorting is used to match last jobs first, and avoid small matches of first nodes
				batches << [k , val]
			end
			batches.reverse.each do |name, batch| 
				if command.include?(name+')') && !string_overlap(matched_regions, name+')', command)
					@dependencies << [name, 'simple', name+')']
				end
				if command.include?("!#{name}*!") && !string_overlap(matched_regions, "!#{name}*!", command)
					@dependencies << [name, '1to1', "!#{name}*!"]
				end
				if command.include?("!#{name}!") && !string_overlap(matched_regions, "!#{name}!", command)
					#command =~ /!#{name}!([^ \n]+)/
					command.scan(/!#{name}!([^ \n]+)/).each do |string_match|
						@dependencies << [name, '*to1', "!#{name}!", string_match.first]
					end
				end
				local_dependencies = command.scan(/#{name}([^\( \n]+)\)/)
				local_dependencies.each do |local_dependency|
					if !string_overlap(matched_regions, "#{name}#{local_dependency.first}"+')', command)
						@dependencies << [name, 'local', "#{name}#{local_dependency.first}"+')', local_dependency.first] 
					end
				end
			end
		end
	end

	def get_string_position(substr, string)
		start = string.index(substr)
		ending = start + substr.length - 1
		range = [start, ending]
		return range
	end

	def string_overlap(matched_regions, substr, string)
		match = false
		range = get_string_position(substr, string)
		if !range.empty?
			matched_regions.each do |start, ending|
				if (range.first >= start && range.first <= ending) || 
					(range.last >= start && range.last <= ending) ||
					(range.first <= start && range.last >= ending)
					match = true
					break
				end
			end
			matched_regions << range 
		end
		return match
	end

	def collect_dinamic_variables(command)
		dinamic_variables = []
		if !command.nil? && command.include?('env_manager')
			command =~ /env_manager "([^"]+)/
			command =~ /env_manager '([^']+)/ if $1.nil?
			if !$1.nil?
				$1.split(';').each do |variable|
					name, value = variable.split('=')
					name.gsub!(' ', '') #Remove spaces
					dinamic_variables << [name, @name]
				end
			end
		end
		return dinamic_variables
	end

	def check_dependencies_with_DinVar(command, dinamic_variables)
		dep = []
		dinamic_variables.each do |var, name|
			dep << [name, 'DinVar'] if command.include?(var)
		end
		return dep
	end

	def replace_dependencies(command, job_dependencies, iter, num)
		if !command.nil?
			command = command.gsub('(*)', "#{iter}") if command.class.to_s == 'String'
			scan_dependencies(command)
			@dependencies.each do |batch_name, dep_type, dep_keyword2replace, dep_info|
				if dep_type == 'simple'
					if @@all_batch[batch_name].parent.nil?
						new_string = batch_name + ')'
					end
					job_dependencies << batch_name					
				elsif dep_type == '1to1'
					if @@all_batch[batch_name].parent.nil? || !@parent.nil?
						dep_name = "#{batch_name}#{@@batch_iterator_relations[batch_name][num]}"
					else
						root_batch = get_root(batch_name)
						selected_jobs = root_batch.get_jobs_by_batch_name(batch_name)
						dep_name = selected_jobs[num].name
					end
					job_dependencies << dep_name
					new_string = dep_name + ')' 
				elsif dep_type == '*to1'
					if @@all_batch[batch_name].parent.nil? || !@parent.nil?
						new_string = @@batch_iterator_relations[batch_name].map{|iter|
							dep_name =  batch_name + iter
							job_dependencies << dep_name
							"#{dep_name})#{dep_info}"
						}.join(' ')
					else
						root_batch = get_root(batch_name)
						selected_jobs = root_batch.get_jobs_by_batch_name(batch_name)
						new_string = selected_jobs.map{|j|
							job_dependencies << j.name
							"#{j.name + ')'}#{dep_info}"
						}.join(' ')
					end
					dep_keyword2replace = "#{dep_keyword2replace}#{dep_info}"
				elsif dep_type == 'local'
					if @@all_batch[batch_name].parent.nil? || !@parent.nil?
						#@@batch_iterator_relations.each do |key,val|
						#	puts "#{key}\t#{val.inspect}"
						#end
						if @@batch_iterator_relations[batch_name].map{|iter| iter.gsub(')','')}.include?(dep_info) #This avoids cross dependencies by similar names, map used for regexp deps
							dep_name = batch_name + dep_info
							job_dependencies << dep_name
							new_string = dep_name + ')'
						end
					else
						dep_name = dep_keyword2replace.gsub(')','') #This avoids cross dependencies by similar names
						#if !@@jobs_names.include?(dep_name)
						job_dependencies << dep_name
						new_string = dep_name + ')'
						#end
					end
				elsif dep_type == 'DinVar'
					job_dependencies << batch_name if batch_name != @name # This condition avoids autodependencies
				end
				job_dependencies.uniq!
				command = command.gsub(dep_keyword2replace, new_string) if dep_type != 'DinVar' && !dep_keyword2replace.nil? && !new_string.nil?
			end
		end
		return command
	end

	def get_root(batch_name)
		root_batch = @@all_batch[batch_name]
		root_batch = root_batch.get_root(root_batch.parent) if !root_batch.nil? && !root_batch.parent.nil?
		return root_batch
	end

	def get_jobs_by_batch_name(batch_name)
		jobs = @jobs.select{|j| j.batch == batch_name}
		return jobs
	end

end
