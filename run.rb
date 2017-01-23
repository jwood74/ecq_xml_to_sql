#!/usr/bin/ruby

require 'nokogiri'

require_relative 'personal_options'
require_relative 'database'
require_relative 'commands'

puts "Auto Election Upload for #{$elec}"
#puts "Checking for RUN in download table"

## TODO insert a check if we should do the process

newf = download_file

if newf == false
	puts "Nothing to download"
	sleep(90)
else
	ProcessXML.loadXML
	
end