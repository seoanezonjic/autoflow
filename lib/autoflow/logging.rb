def parse_log(log_path)
	log = {}
	if Dir.exists?(log_path)
		Dir.entries(log_path).each do |entry|
			next if entry == '.' || entry == '..'
			File.open(File.join(log_path, entry)).each do |line|
				line.chomp!
				name, status, time_int = line.split("\t")
				time = time_int.to_i
				query = log[name]
				if query.nil?
					log[name] = {status => [time]}
				else
					query_status = query[status]
					if query_status.nil?
						query[status] = [time]
					else
						query[status] << time
					end
				end
			end
		end
	end
	log.each do |task, attribs|
		#puts "#{attribs.inspect}"
		set_length = attribs['set'].length
		fill_attrib(attribs, 'start', set_length)
		fill_attrib(attribs, 'end', set_length)
	end
	return log
end

def fill_attrib(attribs, mode, set_length)
	query = attribs[mode]
	if query.nil?
		attribs[mode] = Array.new(set_length, 0)
	elsif query.length < set_length
		(set_length - query.length).times do
			query << 0
		end
	end
end

def write_log(log, log_path, job_relations_with_folders)
	Dir.mkdir(log_path) if !Dir.exists?(log_path)
	job_relations_with_folders.each do |name, folder_deps|
		if !log[name].nil? #Control check when the wk_log folder has been deleted
			folder, deps = folder_deps
			f = File.open([log_path, File.basename(folder)].join('/'), 'w') 
			log[name].each do |mode, times|
				times.each do |time|
					f.puts "#{name}\t#{mode}\t#{time}"
				end
			end
			f.close
		end
	end
end