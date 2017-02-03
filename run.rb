#!/usr/bin/ruby

require 'nokogiri'

require_relative 'options'
require_relative 'database'
require_relative 'commands'

puts "Auto Election Upload for #{$elec}"
#puts "Checking for RUN in download table"

## TODO insert a check if we should do the process

newf = reuse_file
#newf = download_file

if !newf
	puts "Nothing to download"
	sleep(90)
else
	#load_XML(newf)
	if $run_method == 'setup'
		#process_booths(@doc)
		#process_candidates(@doc)
		#process_districts(@doc)
		process_parties(@doc)
		exit

	elsif $run_method == 'results'
		if $mysql_run


		else


		end
	end
end