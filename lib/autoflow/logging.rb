def parse_log(log_path)
	log = {}
	if File.exists?(log_path)
		File.open(log_path).each do |line|
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
	log.each do |task, attribs|
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

def write_log(log, log_path)
	f = File.open(log_path, 'w') 
	log.each do |task, attribs|
		attribs.each do |mode, times|
			times.each do |time|
				f.puts "#{task}\t#{mode}\t#{time}"
			end
		end
	end
	f.close
end