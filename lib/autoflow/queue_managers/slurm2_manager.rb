require 'queue_manager'
class SlurmManager2 < QueueManager
	# SLURM 20 or greater
	def parse_additional_options(string, attribs)
		expresions = %w[%C %T %M %N ]
		values = [attribs[:cpu], attribs[:time], attribs[:mem], attribs[:node]]
		new_string = string.dup
		expresions.each_with_index do |exp, i|
			new_string.gsub!(exp, "#{values[i]}")
		end
		return new_string
	end

	def write_header(id, job, sh_name)
		if !job.attrib[:ntask]
			write_file(sh_name, "#SBATCH --cpus-per-task=#{job.attrib[:cpu]}")
		else
			write_file(sh_name, "#SBATCH --ntasks=#{job.attrib[:cpu]}")
			write_file(sh_name, "#SBATCH --nodes=#{job.attrib[:multinode]}") if job.attrib[:multinode] > 0
		end
		write_file(sh_name,	"#SBATCH --mem=#{job.attrib[:mem]}")
		write_file(sh_name, "#SBATCH --time=#{job.attrib[:time]}")
		write_file(sh_name,	"#SBATCH --constraint=#{job.attrib[:node]}") if !job.attrib[:node].nil?
		write_file(sh_name, '#SBATCH --error=job.%J.err')
		write_file(sh_name, '#SBATCH --output=job.%J.out')
		write_file(sh_name, "#SBATCH --#{job.attrib[:additional_job_options][0]}=#{parse_additional_options(job.attrib[:additional_job_options][1], job.attrib)}") if !job.attrib[:additional_job_options].nil?
		if job.attrib[:ntask]
			write_file(sh_name, 'srun hostname -s > workers') if job.attrib[:cpu_asign] == 'list'
		end
	end


	def submit_job(job, ar_dependencies)
		final_dep = get_all_deps(ar_dependencies)
		dependencies = nil
		dependencies='--dependency=afterok:'+final_dep.join(':') if !final_dep.empty?  
		cmd = "sbatch #{dependencies} #{job.name}.sh"
		STDOUT.puts cmd if @show_submit
		queue_id = get_queue_system_id(system_call(cmd, job.attrib[:exec_folder]))
		return queue_id
	end

	def get_queue_system_id(shell_output)
		queue_id = nil
		shell_output.chomp!
		shell_output =~ /Submitted batch job (\d+)/
		queue_id = $1
		raise("A queue id cannot be obtained. The queue manager has given this message:#{shell_output}") if queue_id.nil?
		return queue_id
	end

	def self.available?(options)
		available = false
		shell_output = system_call("type 'sbatch'", nil, options[:remote], options[:ssh])
		if !shell_output.empty?
			shell_output = system_call("sbatch --version", nil, options[:remote], options[:ssh])
			slurm_version = shell_output.scan(/slurm ([0-9\.]+)/).first.first.split('.').first.to_i # "slurm 17.11.4"
			available = true if slurm_version >= 20
		end
		return available
	end

	def self.priority
		return 100 
	end
end
