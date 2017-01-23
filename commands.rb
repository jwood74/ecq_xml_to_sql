require 'net/http'
require 'zip/zip'

def sync_number(database,elec)
	numberss = SqlDatabase.runQuery("#{database}","select count(*) as syncs from #{elec}_syncs")
	numberr = 1
	numberss.each do | number |
		numberr = number['syncs'] + 1
	end
	log_report(1,"sync numbers is #{numberr}")
	return numberr
end

def download_file
	log_report(1,'Starting the download process. First checking website.')

	syncnumber = sync_number($database,$elec)

	`wget -nv '#{$xml_url}' --output-document=#{$location}/raw/#{syncnumber}.zip`

	Zip::ZipFile.open("#{$location}/raw/#{syncnumber}.zip") { |zip_file|
			zip_file.each { |f|
			f_path=File.join("#{$location}/raw/#{syncnumber}", f.name)
 			FileUtils.mkdir_p(File.dirname(f_path))
 			zip_file.extract(f, f_path) unless File.exist?(f_path)
			}
		}
	newfile = "#{$location}/raw/#{syncnumber}/publicResults.xml"

	puts newfile
	return newfile
end

def sql_upload(sql)
	#If you want to upload data to multiple MySQL servers, replicatate the below line for each server
	#You will also need to add a Class for each server in the database.rb
	SqlDatabase.runQuery($database,sql)
end


def log_report(level,message)
	#Really simple function to enable log reporting
	#Adjust the log level in personal_options.rb and program will print any logs less than your number
	if level.to_i <= $report_level.to_i
		puts message
	end
end

class ProcessXML
	def load_XML(xmlfile)
		@xmlfile = xmlfile
		log_report(5,"Connecting the XML file @ #{@xmlfile}")

		@doc = Nokogiri::XML(File.open(@xmlfile)) do || config
			config.noblanks
		end
		@doc.remove_namespaces!
		return @doc
	end
end