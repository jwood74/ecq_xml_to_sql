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
	sql_upload("insert into " + $elec + "_syncs (updated) values (\'#{updated}\');")
	return @doc
end

def reuse_file
	##This function is used to connect an existing XML file
	##during testing, rather than download new ones
	@xmlfile = $location + "/raw/2/publicResults.xml"
	log_report(5,@xmlfile)

	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	updated = @doc.at_xpath("//generationDateTime").text
	return @doc
end

def create_syncs_table
	log_report(9,"Checking if Sync Table Exists")
	tbl_check = "select * from #{$elec}_syncs;"
	begin
		sql_upload(tbl_check).first
	rescue
		log_report(9,"Sync Table doesn't exist. Creating one now")
		sql = "CREATE TABLE #{$elec}_syncs (`sync` int(11) NOT NULL AUTO_INCREMENT, `updated` datetime DEFAULT NULL, `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`sync`)) AUTO_INCREMENT=1;"
		sql_upload(sql)
	end
end

def create_views
	sql = "drop view if exists vw_#{$elec}_results; CREATE ALGORITHM=UNDEFINED DEFINER=`#{$mysql_user}`@`localhost` SQL SECURITY DEFINER VIEW `vw_#{$elec}_results` AS select `d`.`#{$area}_id` AS `#{$area}_id`,`d`.`#{$area}` AS `#{$area}`,`b`.`booth_id` AS `booth_id`,`b`.`name` AS `booth_name`,`c`.`ballot_position` AS `ballot_position`,`c`.`ballot_name` AS `ballot_name`,`c`.`party` AS `party`,`v`.`vote_type` AS `vote_type`,`v`.`type` AS `type`,`r`.`votes` AS `votes` from ((((`#{$elec}_results` `r` join `#{$elec}_booths` `b` on(((`r`.`#{$area}_id` = `b`.`#{$area}_id`) and (`r`.`booth_id` = `b`.`booth_id`)))) join `#{$elec}_#{$area}s` `d` on((`r`.`#{$area}_id` = `d`.`#{$area}_id`))) join `#{$elec}_candidates` `c` on(((`r`.`#{$area}_id` = `c`.`#{$area}_id`) and (`r`.`candidate_id` = `c`.`ballot_position`)))) join `#{$elec}_vote_types` `v` on((`r`.`vote_type` = `v`.`vote_type`)));"
	sql_upload(sql)
	sql = "drop view if exists vw_#{$elec}_results_booth; CREATE ALGORITHM=UNDEFINED DEFINER=`#{$mysql_user}`@`localhost` SQL SECURITY DEFINER VIEW `vw_#{$elec}_results_booth` AS select `r`.`#{$area}_id` AS `#{$area}_id`,`r`.`#{$area}` AS `#{$area}`,`r`.`booth_id` AS `booth_id`,`r`.`booth_name` AS `booth_name`,sum(if(((`r`.`party` = 'ALP') and (`r`.`vote_type` = 1)),`r`.`votes`,0)) AS `fp_alp`,sum(if(((`r`.`party` = 'LNP') and (`r`.`vote_type` = 1)),`r`.`votes`,0)) AS `fp_lnp`,sum(if(((`r`.`party` = 'GRN') and (`r`.`vote_type` = 1)),`r`.`votes`,0)) AS `fp_grn`,sum(if(((`r`.`party` = 'ONP') and (`r`.`vote_type` = 1)),`r`.`votes`,0)) AS `fp_onp`,sum(if(((`r`.`party` not in ('ALP','LNP','GRN','ONP')) and (`r`.`vote_type` = 1)),`r`.`votes`,0)) AS `fp_oth`,sum(if((`r`.`vote_type` = 1),`r`.`votes`,0)) AS `fp_votes`,sum(if(((`r`.`party` = 'ALP') and (`r`.`vote_type` = 2)),`r`.`votes`,0)) AS `tcp_alp`,sum(if(((`r`.`party` = 'LNP') and (`r`.`vote_type` = 2)),`r`.`votes`,0)) AS `tcp_lnp`,sum(if((`r`.`vote_type` = 2),`r`.`votes`,0)) AS `tcp_votes` from `vw_#{$elec}_results` `r` group by `r`.`#{$area}_id`,`r`.`booth_id`;"
	sql_upload(sql)
end

def process_booths(doc)
	@doc = doc

	puts "Processing the booths."
	#NOTE that ECQ does not put all booth data in their XML feed
	#This code will grab what it can
	#The remaining data will be on the ECQ website
	#e.g. http://results.ecq.qld.gov.au/elections/state/State2015/XML/Aspley_Booth.xml

	##create a booth table
	sql = "drop table if exists #{$elec}_booths; CREATE TABLE #{$elec}_booths ( #{$area}_id int(11) NOT NULL, booth_id int(11) NOT NULL, name varchar(50) DEFAULT NULL, type varchar(2) DEFAULT NULL, `in` varchar(3) DEFAULT NULL, formal int(11) DEFAULT NULL, informal int(11) DEFAULT NULL, total int(11) DEFAULT NULL, exhaust int(11) DEFAULT NULL, orig_name varchar(255) DEFAULT NULL, description varchar(100) DEFAULT NULL, address varchar(100) DEFAULT NULL, suburb varchar(40) DEFAULT NULL, latitude decimal(15,8) DEFAULT NULL, longitude decimal(15,8) DEFAULT NULL, PRIMARY KEY (#{$area}_id,booth_id), KEY idx_booth_id (booth_id) USING BTREE, KEY idx_name (name) USING BTREE );"
	sql << "truncate #{$elec}_booths; insert into #{$elec}_booths (#{$area}_id,booth_id,name,type) values "

	district = @doc.xpath("//#{$area}s/#{$area}")
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

	sql = "drop table if exists #{$elec}_candidates; create table #{$elec}_candidates (#{$area}_id int(2) NOT NULL, ballot_position int(2) NOT NULL, ballot_name varchar(255) DEFAULT NULL, surname varchar(255) DEFAULT NULL, givennames varchar(255) DEFAULT NULL, party varchar(4) DEFAULT NULL, gender varchar(1) DEFAULT NULL, sitting varchar(5) DEFAULT NULL, PRIMARY KEY (`#{$area}_id`,`ballot_position`));"
	sql << "truncate #{$elec}_candidates; insert into #{$elec}_candidates (#{$area}_id, ballot_position, ballot_name, party, gender, sitting) values "

	district = @doc.xpath("//#{$area}s/#{$area}")
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

	puts "Processing the #{$area}s."

	sql = "drop table if exists #{$elec}_#{$area}s; CREATE TABLE #{$elec}_#{$area}s (  #{$area}_id int(11) NOT NULL,  #{$area} varchar(255) DEFAULT NULL,  enrolment int(5) DEFAULT NULL,  counted float(5,2) DEFAULT NULL,  declared_party varchar(5) DEFAULT NULL,  declared_name varchar(50) DEFAULT NULL,  PRIMARY KEY (#{$area}_id),  UNIQUE KEY idx_electorate (#{$area}) USING BTREE);"
	sql << "truncate #{$elec}_#{$area}s; insert into #{$elec}_#{$area}s (#{$area}_id, #{$area}, enrolment, counted, declared_party, declared_name) values "

	district = @doc.xpath("//#{$area}s/#{$area}")
	district.each do | dis |
		if dis['percentRollCounted'].nil?
			percount = 'NULL'
		else
			percount = dis['percentRollCounted']
		end
		sql << "(#{dis['number']}, \"#{dis['name']}\", #{dis['enrolment']}, #{percount}, \"#{dis['declaredPartyCode']}\", \"#{dis['declaredBallotName']}\"),"
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "#{$area}s complete."
end

def process_parties(doc)
	@doc = doc

	puts "Processing parties."

	sql = "drop table if exists #{$elec}_parties; CREATE TABLE #{$elec}_parties ( code varchar(5) NOT NULL, name varchar(50) DEFAULT NULL, PRIMARY KEY (`code`));"
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

def create_results_table
	begin
		sql_upload("select * from #{$elec}_results;").first
	rescue
		sql = "CREATE TABLE #{$elec}_results ( `id` int(11) NOT NULL AUTO_INCREMENT, `#{$area}_id` int(2) DEFAULT NULL, `booth_id` int(4) DEFAULT NULL, `candidate_id` int(4) DEFAULT NULL, `vote_type` int(4) DEFAULT NULL, `votes` int(4) DEFAULT NULL, last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (`id`), UNIQUE KEY `idx_unique` (`#{$area}_id`,`booth_id`,`candidate_id`,`vote_type`), KEY `idx_dcv` (`#{$area}_id`,`candidate_id`,`vote_type`), KEY `idx_bcv` (`booth_id`,`candidate_id`,`vote_type`), KEY `idx_cv` (`candidate_id`,`vote_type`), KEY `idx_v` (`vote_type`));"
		sql_upload(sql)
		puts "Results table complete"
	end
end

def create_votetypes_table
	begin
		sql_upload("select * from #{$elec}_vote_types;").first
	rescue
		sql = "CREATE TABLE #{$elec}_vote_types (`vote_type` int(11) NOT NULL AUTO_INCREMENT,`type` varchar(15) DEFAULT NULL, `description` varchar(60) DEFAULT NULL, PRIMARY KEY (`vote_type`), UNIQUE KEY `idx_type` (`type`));"
		sql_upload(sql)
		sql_upload("INSERT INTO #{$elec}_vote_types (`vote_type`, `type`, `description`) VALUES ('1', 'primary', 'primary votes for polling place'),('2', 'tcp', 'two candidate preferred for polling place');")
		puts "VoteTypes table complete"
	end
end

def process_primaries(doc)
	@doc = doc

	puts "Processing the primary results"
	sql = "INSERT INTO #{$elec}_results (#{$area}_id, booth_id, candidate_id, vote_type, votes) VALUES "

	district = @doc.xpath("//#{$area}s/#{$area}")
	district.each do | dis |
		booth = dis.xpath("./booths/booth")
		booth.each do | bth |
			boothcand = bth.xpath("./boothCandidates/boothCandidate")
			boothcand.each do | cand |
				prim = cand.at_xpath("./primaryVotes").text
				sql << "(#{dis['number']},#{bth['id']},#{cand['ballotOrderNumber']},1,#{prim}),"
			end
		end
	end
	sql  = sql[0..-2]	#chop off the last comma
	sql << "ON DUPLICATE KEY UPDATE votes = values(votes);"
	sql_upload(sql)
	puts "Primary complete."
end

def process_tcp(doc)
	@doc = doc

	puts "Processing the tcp results"
	sql = "INSERT INTO #{$elec}_results (#{$area}_id, booth_id, candidate_id, vote_type, votes) VALUES "

	district = @doc.xpath("//#{$area}s/#{$area}")
	district.each do | dis |
		booth = dis.xpath("./booths/booth")
		booth.each do | bth |
			boothcand = bth.xpath("./boothCandidates/boothCandidate")
			boothcand.each do | cand |
				tcp = cand.at_xpath("./n2cpVotes")
				if tcp.nil? || tcp == 0
					tcp = "NULL"
				else
					tcp = tcp.text
				end
				sql << "(#{dis['number']},#{bth['id']},#{cand['ballotOrderNumber']},2,#{tcp}),"
			end
		end
	end
	sql  = sql[0..-2]	#chop off the last comma
	sql << "ON DUPLICATE KEY UPDATE votes = values(votes);"
	sql_upload(sql)
	puts "TCP complete."
end