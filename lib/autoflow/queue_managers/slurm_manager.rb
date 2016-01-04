require 'queue_manager'
class SlurmManager < QueueManager
	def write_header(id, job, sh_name)
		if !job.attrib[:ntask]
			write_file(sh_name, "#SBATCH --cpus=#{job.attrib[:cpu]}")
		else
			write_file(sh_name, "#SBATCH --ntasks=#{job.attrib[:cpu]}")
			write_file(sh_name, "#SBATCH --nodes=#{job.attrib[:multinode]}") if job.attrib[:multinode] > 0
			write_file(sh_name, 'srun hostname -s > workers') if job.attrib[:cpu_asign] == 'list'
		end
		write_file(sh_name,	"#SBATCH --mem=#{job.attrib[:mem]}")
		write_file(sh_name, "#SBATCH --time=#{job.attrib[:time]}")
		write_file(sh_name,	"#SBATCH --constraint=#{job.attrib[:node]}") if !job.attrib[:node].nil?
		write_file(sh_name, '#SBATCH --error=job.%J.err')
		write_file(sh_name, '#SBATCH --output=job.%J.out')
	end

	def submit_job(job, ar_dependencies)
		final_dep = get_all_deps(ar_dependencies)
		dependencies = nil
		dependencies='--dependency=afterok:'+final_dep.join(':') if !final_dep.empty?  
		cmd = "sbatch #{dependencies} #{job.name}.sh"
		queue_id = get_queue_system_id(system_call(cmd, job.attrib[:exec_folder]))
		return queue_id
	end

	def get_queue_system_id(shell_output)
		queue_id = nil
		shell_output.chomp!
		fields = shell_output.split(' ')
		queue_id = fields[3]
		return queue_id
	end

	def self.available?(options)
		available = TRUE
		shell_output = system_call("type 'sbatch'", nil, options[:remote], options[:ssh])
		available = FALSE if shell_output.empty?
		return available
	end

	def self.priority
		return 100 
	end
end