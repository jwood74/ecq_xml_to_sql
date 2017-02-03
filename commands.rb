require 'net/http'
require 'zip/zip'

def sync_number(database,elec)
	##A table is kept to record each sync
	##This function simply finds out what number we are up to so that the ZIP and XML file
	## can be saved with the appropriate number
	numberss = SqlDatabase.runQuery("#{database}","select count(*) as syncs from #{elec}_syncs")
	numberr = 1
	numberss.each do | number |
		numberr = number['syncs'] + 1
	end
	log_report(1,"sync numbers is #{numberr}")
	return numberr
end

def download_file
	##Downlaod the XML zip file from location specificed in personal_options
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

def load_XML(xmlfile)
	@xmlfile = xmlfile
	log_report(5,"Connecting the XML file @ #{@xmlfile}")

	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	updated = @doc.at_xpath("//generationDateTime").text
	sql_upload("insert into results." + $elec + "_syncs (updated) values (\'#{updated}\');")
	return @doc
end

def reuse_file
	##This function is used to connect an existing XML file
	##during testing, rather than download new ones
	@xmlfile = $location + "/raw/3/publicResults.xml"
	log_report(5,@xmlfile)

	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	updated = @doc.at_xpath("//generationDateTime").text
	return @doc
end

def process_booths(doc)
	@doc = doc

	puts "Processing the booths."
	#NOTE that ECQ does not put all booth data in their XML feed
	#This code will grab what it can
	#The remaining data will be on the ECQ website
	#e.g. http://results.ecq.qld.gov.au/elections/state/State2015/XML/Aspley_Booth.xml

	##create a booth table
	sql = "drop table if exists #{$elec}_booths; CREATE TABLE #{$elec}_booths ( district_id int(11) NOT NULL, booth_id int(11) NOT NULL, name varchar(50) DEFAULT NULL, type varchar(2) DEFAULT NULL, `in` varchar(3) DEFAULT NULL, formal int(11) DEFAULT NULL, informal int(11) DEFAULT NULL, total int(11) DEFAULT NULL, exhaust int(11) DEFAULT NULL, orig_name varchar(255) DEFAULT NULL, description varchar(100) DEFAULT NULL, address varchar(100) DEFAULT NULL, suburb varchar(40) DEFAULT NULL, latitude decimal(15,8) DEFAULT NULL, longitude decimal(15,8) DEFAULT NULL, PRIMARY KEY (district_id,booth_id), KEY idx_booth_id (booth_id) USING BTREE, KEY idx_name (name) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_booths; insert into #{$elec}_booths (district_id,booth_id,name,type) values "

	district = @doc.xpath("//districts/district")
	district.each do | dis |
		booth = dis.xpath("./booths/booth")
		booth.each do | bth |
			sql << "(#{dis['number']},#{bth['id']},\"#{bth['name']}\",\"#{bth['typeCode']}\"),"
		end
	end
	sql  = sql[0..-2]	#chop off the last comma
	sql << ";"
	sql_upload(sql)
	puts "Booths complete."
end

def process_candidates(doc)
	@doc = doc

	puts "Processing the candidates."
	#NOTE that ECQ provides limited candidate details in the XML
	#Futher details can be found on their website and candidates table updated

	sql = "drop table if exists #{$elec}_candidates; create table #{$elec}_candidates (district_id int(2) NOT NULL, ballot_position int(2) NOT NULL, ballot_name varchar(255) DEFAULT NULL, surname varchar(255) DEFAULT NULL, givennames varchar(255) DEFAULT NULL, party varchar(4) DEFAULT NULL, gender varchar(1) DEFAULT NULL, sitting varchar(5) DEFAULT NULL, PRIMARY KEY (`district_id`,`ballot_position`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_candidates; insert into #{$elec}_candidates (district_id, ballot_position, ballot_name, party, gender, sitting) values "

	district = @doc.xpath("//districts/district")
	district.each do | dis |
		candidate = dis.xpath("./candidates/candidate")
		candidate.each do | cand |
			sql << "(#{dis['number']}, #{cand['ballotOrderNumber']}, \"#{cand['ballotName']}\", \"#{cand['party']}\", \"#{cand['gender']}\", \"#{cand['sitting']}\"),"
		end
	end
	sql = sql[0..-2]	#chop off the last comma
	sql << ";"
	sql_upload(sql)
	puts "Candidates complete."

end

def process_districts(doc)
	@doc = doc

	puts "Processing the districts."

	sql = "drop table if exists #{$elec}_districts; CREATE TABLE #{$elec}_districts (  district_id int(11) NOT NULL,  district varchar(255) DEFAULT NULL,  enrolment int(5) DEFAULT NULL,  counted float(5,2) DEFAULT NULL,  declared_party varchar(5) DEFAULT NULL,  declared_name varchar(50) DEFAULT NULL,  PRIMARY KEY (district_id),  UNIQUE KEY idx_electorate (district) USING BTREE) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_districts; insert into #{$elec}_districts (district_id, district, enrolment, counted, declared_party, declared_name) values "

	district = @doc.xpath("//districts/district")
	district.each do | dis |
		sql << "(#{dis['number']}, \"#{dis['name']}\", #{dis['enrolment']}, #{dis['percentRollCounted']}, \"#{dis['declaredPartyCode']}\", \"#{dis['declaredBallotName']}\"),"
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "Districts complete."
end

def process_parties(doc)
	@doc = doc

	puts "Processing parties."

	sql = "drop table if exists #{$elec}_parties; CREATE TABLE #{$elec}_parties ( code varchar(5) NOT NULL, name varchar(50) DEFAULT NULL, PRIMARY KEY (`code`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_parties; insert into #{$elec}_parties (code, name) values "

	party = @doc.xpath("//parties/party")
	party.each do |part|
		sql << "(\"#{part['code']}\", \"#{part['name']}\"),"
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "Parties complete."
end