#require 'program'
#require 'batch'

require 'win32console' if !ENV['OS'].nil? && ENV['OS'].downcase.include?('windows')
require 'colorize'
class Stack
	attr_accessor :jobs, :exec_folder, :persist_variables

##########################################################################################
## PARSE TEMPLATE
##########################################################################################
	def initialize(exec_folder, options)
		Batch.set_general_attrib({
			:cpu => options[:cpus], 
			:mem => options[:memory], 
			:time => options[:time],
			:node => options[:node_type],
			:multinode => options[:use_multinode],
			:ntask => options[:use_ntasks],
			:additional_job_options => options[:additional_job_options]
		})
		@@folder_name = :program_name
		@@folder_name = :job_name if options[:key_name]
		@commands = {}	
		@variables = {}
		@persist_variables = {}
		@@all_jobs_relations = {}
		@exec_folder = exec_folder #TODO move this to queue_manager
		@do_retry = options[:retry]
		@options = options
		@workflow = options[:workflow]
		@external_variables= options[:Variables] 
		@jobs = {}
	end
	
	def parse!
		#Clean template
		@workflow.gsub!(/\#.+$/,'')	#Delete comments
		@workflow.gsub!("\t",'')		#Drop tabs
		@workflow.gsub!(/\n+/,"\n")	#Drop empty lines
		@workflow.gsub!(/^\s*/,'')

		#Parse template
		variables_lines = []
		persist_variables_lines = []
		node_lines = []

		node_beg = false		
		@workflow.each_line do |line|
			node_beg = true if line.include?('{') 	# This check the context of a variable
			if line.include?('}')					# if a variable is within a node,
				if node_beg							# we consider tha is a bash variable not a static autoflow variable
					node_beg = false
				else
					node_beg = true
				end
			end
			if line =~ /^\$/ && !node_beg
				variables_lines << line
			elsif line =~ /^\@/
				persist_variables_lines << line.gsub('@','')
			else
				node_lines << line
			end
		end
		load_variables(variables_lines, @variables)
		load_variables(@external_variables, @variables)
		load_variables(persist_variables_lines, @persist_variables)
		parse_nodes(node_lines)
		@jobs = get_jobs_relations
	end

	def load_variables(variables_lines, variable_type)
		if !variables_lines.nil?
			variables_lines.each do |line|
				line.chomp!
				line.gsub!(/\s/,'')
				pairs = line.split(',')
				pairs.each do |pair|
					#pair =~ /(.+)=(.+)/
					#variable_type[$1] = $2
					var, value = pair.split('=', 2)
					variable_type[var] = value
				end
			end
		end
	end

	def scan_nodes(execution_lines)
		template_executions = execution_lines.join('')
		replace_variables(template_executions)
		# $1 => tag, $2 => initialize, $3 => main command
		#executions = template_executions.scan(/(^.+\))\s{0,}\{\s{0,}([^\?]{0,})\s{0,}\?\s([^\}]{1,})\s{0,}\}/)
#=begin
		executions = [] # tag, initialize, main_command
		states = {} #name => [state, id(position)]
					#t => tag, i => initialize , c => command
		open_nodes = []
		template_executions.each_line do |line|
			line.strip! #Clean al whitespaces at beginning and the end of string
			node = states[open_nodes.last] if !open_nodes.empty?
			if line.empty?
				next
			# Create nodes and asign nodes states
			#----------------------------------------
			elsif line =~ /(\S*\)){$/ #Check tag and create node
				name = $1
				executions << [name, '', ''] # create node
				states[name] = [:i, executions.length - 1]
				open_nodes << name
			elsif line == '?' #Check command
				node[0] = :c
			elsif line == '}' #Close node
				finished_node = open_nodes.pop
				if !open_nodes.empty?
					parent_node = states[open_nodes.last].last #position
					child_node = states[finished_node].last
					parent_execution = executions[parent_node]
					if parent_execution[2].class == String   
						parent_execution[2] = [child_node]
					else
						parent_execution[2] << child_node
					end
				end
			# Add lines to nodes
			#----------------------
			elsif states[open_nodes.last].first == :i #Add initialize line
				executions[node.last][1] << line +"\n"
			elsif states[open_nodes.last].first == :c #Add command line
				executions[node.last][2] << line +"\n"			
			end
		end
#=end
		return executions
	end

	def replace_variables(string)
		@variables.each do |name, value|
			string.gsub!(name, value)
		end
	end

	def parse_nodes(execution_lines)
		dinamic_variables = []
		nodes = scan_nodes(execution_lines)
		nodes = create_ids(nodes)
		
		#nodes.each do |tag, init, command, index|
		#	puts "#{tag.colorize(:red)}\t#{index}\n#{('-'*tag.length).colorize(:red)}\n#{init.chomp.colorize(:blue)}\n#{command.to_s.colorize(:green)}"
		#end

		nodes.each do |tag, init, command, index| #Takes the info of each node of workflow to create the job
			# Set batch	
			new_batch = Batch.new(tag, init, command, index, @exec_folder)
			dinamic_variables.concat(new_batch.handle_dependencies(dinamic_variables))
			@commands[new_batch.name] = new_batch
		end

		# link each parent batch to a child batch
		@commands.each do |name, batch|
			batch.asign_child_batch
		end
	end

	def create_ids(nodes)
		nodes.each_with_index do |node, i|
			node << i
		end
		return nodes
	end

	def set_dependencies_path(job) #TODO move this to queue_manager
		job.dependencies.sort{|d1, d2| d2.length <=> d1.length}.each do |dep|
			path =  @@all_jobs_relations[dep]
			job.initialization.gsub!(dep+')', path) if !job.initialization.nil?
			job.parameters.gsub!(dep+')', path)
		end
	end

	def asign_folder(job) #TODO move this to queue_manager
		folder = nil
		if job.attrib[:folder]
			if @@folder_name == :program_name
				program = File.join(job.attrib[:exec_folder], job.parameters.split(' ', 2).first) 			
				count = 0
				folder = program + "_#{"%04d" % count}"
				while @@all_jobs_relations.values.include?(folder)
					folder = program + "_#{"%04d" % count}"
					count += 1
				end
			elsif @@folder_name == :job_name
				folder = File.join(job.attrib[:exec_folder], job.name)
			end
		else
			folder = job.attrib[:exec_folder]
		end
		@@all_jobs_relations[job.name.gsub(')','')] = folder
		job.attrib[:exec_folder] = folder 
	end
	
	def get_jobs
		jobs =[]
		@commands.each do |name, batch|
			next if batch.has_jobs? #parent batch (intermediates)
			batch.get_jobs.each do |j|
				folder = asign_folder(j) #TODO move this to queue_manager
				jobs << [j.name, j]
			end

		end
		jobs.each do |j_name, job| #TODO move this to queue_manager
			set_dependencies_path(job)
			j_name.gsub!(')','') #Clean function characters on name
			job.name.gsub!(')','')
		end
		return jobs
	end


	def get_jobs_relations
		hash = {}
		get_jobs.each do |name, job|
			hash[name] = job
		end
		return hash
	end

##########################################################################################
## WORKFLOW REPRESENTATION
##########################################################################################

	def inspect
		@jobs.each do |id, job|
			puts "#{id} > #{job.inspect}\t#{job.attrib[:done]}\n\t\e[32m#{job.dependencies.join("\n\t")}\e[0m"
		end
	end

	def draw(name, name_type)
		representation_type = '_structural'
		representation_type = '_semantic' if name_type.include?('t')
		if name_type.include?('b')
			representation_type << '_simplified'
			set = @commands
		else
			set = @jobs
		end
		name.gsub!(/\.\S+/,'')
		file = File.open(name+representation_type+'.dot','w')
		file.puts 'digraph G {', 'node[shape=box]'
		all_dependencies = []
		all_tag = []
		set.each do |id, tag|
			if name_type.include?('b')
				tag_name = tag.main_command.split(' ').first+"_#{tag.id}"
			else 
				tag_name = File.basename(tag.attrib[:exec_folder])
			end
			tag_name = id if name_type.include?('t')
			tag_name = tag_name + '(*)' if name_type.include?('b') && tag.iterator.length > 1
			
			all_tag << tag_name
			if tag.dependencies.length > 0
				tag.dependencies.each do |dependencie, type, string|
					if name_type.include?('b')
						dependencie_name = set[dependencie].main_command.split(' ').first+"_#{set[dependencie].id}"
					else 
						dependencie_name = File.basename(set[dependencie].attrib[:exec_folder])				
					end
					dependencie_name = dependencie if name_type.include?('t')

					dependencie_name = dependencie_name + '(*)' if name_type.include?('b') && set[dependencie].iterator.length > 1
					all_dependencies << dependencie_name

					file.puts "\"#{dependencie_name}\"-> \"#{tag_name}\""
				end
			else
				file.puts "\"#{tag_name}\"[color=black, peripheries=2, style=filled, fillcolor=yellow]"
			end
		end
		all_tag.keep_if{|tag| !all_dependencies.include?(tag)}
		all_tag.each do |tag|
			if name_type.include?('b')
				if !name_type.include?('f')
					tag = tag + '(*)'  if !tag.include?('(*)') && set[tag].iterator.length > 1
				else
					id = tag.reverse.split('_',2).first.reverse.to_i
					batch = nil
					set.each do |id,tag|
						batch = tag if tag.id = id 
					end
					tag = tag + '(*)'  if batch.iterator.length > 1
				end
			end
			file.puts "\"#{tag}\"[fontcolor=white, color=black, style=filled]"
		end
		file.puts '}'
		file.close
		system('dot -Tpdf '+name+representation_type+'.dot -o '+name+representation_type+'.pdf')
	end
	
end
