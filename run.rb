#!/usr/bin/ruby

require 'nokogiri'

require_relative 'wa_options'
require_relative 'database'
require_relative 'commands'

puts "Auto Election Upload for #{$elec}"
#puts "Checking for RUN in download table"

## TODO insert a check if we should do the process

create_syncs_table				#only creates if dones't exist

#newf = reuse_file
newf = download_file

if !newf
	puts "Nothing to download"
else
	load_XML(newf)
	if $run_method == 'setup'
		#process_booths(@doc)
		#process_districts(@doc)

		#process_candidates(@doc)

		#process_parties(@doc)

		#add_special_booths(@doc)
		#create_results_table	#only creates if doesn't exist
		#create_votetypes_table	#only creates if doesn't exist
		#create_views			#drops view and recreates
	elsif $run_method == 'results'
		process_primaries(@doc)
		process_informals(@doc)
		process_tcp(@doc)
		process_exhaust(@doc)
	end
end