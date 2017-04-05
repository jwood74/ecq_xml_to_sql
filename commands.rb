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

	`wget -nv '#{$xml_url}' --output-document=#{$location}/raw/#{syncnumber}.xml`

	newfile = "#{$location}/raw/#{syncnumber}.xml"

	puts newfile
	return newfile
end

def sql_upload(sql)
	#If you want to upload data to multiple MySQL servers, replicatate the below line for each server
	#You will also need to add a Class for each server in the database.rb

	SqlDatabase.runQuery($database,sql)
	WADatabase.runQuery('election_night',sql)
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

	sql_upload("insert into " + $elec + "_syncs (sync) values (NULL);")
	return @doc
end

def reuse_file
	##This function is used to connect an existing XML file
	##during testing, rather than download new ones
	@xmlfile = $location + "/raw/17.xml"
	log_report(5,@xmlfile)

	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	return @doc
end

def create_syncs_table
	tbl_check = "select * from #{$elec}_syncs;"
	begin
		sql_upload(tbl_check).first
	rescue
		sql = "CREATE TABLE #{$elec}_syncs (`sync` int(11) NOT NULL AUTO_INCREMENT, `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`sync`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;"
		sql_upload(sql)
	end
end

def create_views
	sql = "drop view if exists vw_#{$elec}_results; CREATE VIEW `vw_#{$elec}_results` AS select `d`.`district_id` AS `district_id`,`d`.`district` AS `district`,`b`.`booth_id` AS `booth_id`,`b`.`name` AS `booth_name`,`c`.`ballot_position` AS `ballot_position`,`c`.`ballot_name` AS `ballot_name`,`c`.`party` AS `party`,`v`.`vote_type` AS `vote_type`,`v`.`type` AS `type`,`r`.`votes` AS `votes` from ((((`#{$elec}_results` `r` join `#{$elec}_booths` `b` on(((`r`.`district_id` = `b`.`district_id`) and (`r`.`booth_id` = `b`.`booth_id`)))) join `#{$elec}_districts` `d` on((`r`.`district_id` = `d`.`district_id`))) join `#{$elec}_candidates` `c` on(((`r`.`district_id` = `c`.`district_id`) and (`r`.`candidate_id` = `c`.`ballot_position`)))) join `#{$elec}_vote_types` `v` on((`r`.`vote_type` = `v`.`vote_type`)));"
	sql_upload(sql)
	sql = "DROP VIEW IF EXISTS vw_#{$elec}_results_booth; CREATE VIEW `vw_#{$elec}_results_booth` AS SELECT `r`.`district_id` AS `district_id` , `r`.`district` AS `district` , `r`.`booth_id` AS `booth_id` , `r`.`booth_name` AS `booth_name` , sum( IF(((`r`.`party` = 'ALP') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_alp` , sum( IF(((`r`.`party` = 'LIB') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_lib` , sum( IF(((`r`.`party` = 'NAT') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_nat` , sum( IF(((`r`.`party` = 'GRN') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_grn` , sum( IF(((`r`.`party` = 'PHON') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_onp` , sum( IF((( `r`.`party` NOT IN( 'ALP' , 'LIB' , 'NAT' , 'GRN' , 'ONP' , 'INF')) AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `fp_oth` , sum( IF(((`r`.`party` = 'INF') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `informal` , sum( IF(((`r`.`party` <> 'INF') AND(`r`.`vote_type` = 1)) , `r`.`votes` , 0)) AS `formal` , sum( IF((`r`.`vote_type` = 1) , `r`.`votes` , 0)) AS `total` , sum( IF(((`r`.`party` = 'EXH') AND(`r`.`vote_type` = 2)) , `r`.`votes` , 0)) AS `exhaust` , sum( IF(((`r`.`party` = 'ALP') AND(`r`.`vote_type` = 2)) , `r`.`votes` , 0)) AS `tcp_alp` , sum( IF(((`r`.`party` IN('LIB' , 'NAT')) AND(`r`.`vote_type` = 2)) , `r`.`votes` , 0)) AS `tcp_lnp` , sum( IF((`r`.`vote_type` = 2) AND r.party <> 'EXH' , `r`.`votes` , 0)) AS `tcp_votes` FROM `vw_#{$elec}_results` `r` GROUP BY `r`.`district_id` , `r`.`booth_id`;"
	sql_upload(sql)
end

def process_booths(doc)
	@doc = doc

	puts "Processing the booths."

	##create a booth table
	sql = "drop table if exists #{$elec}_booths; CREATE TABLE #{$elec}_booths ( district_id varchar(5) NOT NULL, booth_id varchar(5) NOT NULL, name varchar(70) DEFAULT NULL, type varchar(20) DEFAULT NULL, address varchar(150) DEFAULT NULL, latitude decimal(15,8) DEFAULT NULL, longitude decimal(15,8) DEFAULT NULL, venueid varchar(50) DEFAULT NULL, PRIMARY KEY (district_id,booth_id), KEY idx_booth_id (booth_id) USING BTREE, KEY idx_name (name) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_booths; insert into #{$elec}_booths (district_id,booth_id,name,type,latitude,longitude,venueid) values "

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		booth = dis.xpath("./OrdinaryPollingPlace")
		booth.each do | bth |
			sql << "(\"#{dis['Code']}\",#{bth['Number']},\"#{bth['Name']}\",'ordinary',#{bth['Latitude']},#{bth['Longitude']},\"#{bth['VenueId']}\"),"
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

	sql = "drop table if exists #{$elec}_candidates; create table #{$elec}_candidates (district_id varchar(5) NOT NULL, ballot_position int(2) NOT NULL, ballot_name varchar(255) DEFAULT NULL, surname varchar(255) DEFAULT NULL, givennames varchar(255) DEFAULT NULL, party varchar(4) DEFAULT NULL, sitting varchar(5) DEFAULT NULL, PRIMARY KEY (`district_id`,`ballot_position`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_candidates; insert into #{$elec}_candidates (district_id, ballot_position, ballot_name, surname, givennames, party, sitting) values "

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		candidate = dis.xpath("./LA/Candidate")
		candidate.each do | cand |
			sql << "(\"#{dis['Code']}\", #{cand['BallotPaperOrder']}, \"#{cand['BallotPaperName']}\", \"#{cand['LastName']}\", \"#{cand['FirstName']}\", \"#{cand['RegisteredPartyAbbreviation']}\", \"#{cand['SittingMember']}\"),"
		end
	end
	sql = sql[0..-2]	#chop off the last comma
	sql << ";"
	sql << "insert into #{$elec}_candidates select district_id, 98, 'Informal', NULL, NULL, 'INF', 'FALSE' from #{$elec}_districts d UNION select district_id, 99, 'Exhuast', NULL, NULL, 'EXH', 'FALSE' from #{$elec}_districts d;"
	sql_upload(sql)
	puts "Candidates complete."

end

def process_districts(doc)
	@doc = doc

	puts "Processing the districts."

	sql = "drop table if exists #{$elec}_districts; CREATE TABLE #{$elec}_districts (  district_id varchar(5) NOT NULL,  district varchar(255) DEFAULT NULL,  enrolment int(5) DEFAULT NULL,  PRIMARY KEY (district_id),  UNIQUE KEY idx_electorate (district) USING BTREE) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	sql << "truncate #{$elec}_districts; insert into #{$elec}_districts (district_id, district, enrolment) values "

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		sql << "(\"#{dis['Code']}\", \"#{dis['Name']}\", #{dis['Enrolment']}),"
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

	party = @doc.xpath("//RegisteredParty")
	party.each do |part|
		sql << "(\"#{part['Abbreviation']}\", \"#{part['BallotPaperName']}\"),"
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "Parties complete."
end

def add_special_booths(doc)
	@doc = doc

	puts "Let's add those special booths."

	sql = "INSERT INTO #{$elec}_booths( district_id , booth_id , `name` , type) SELECT district_id , 'SIR' , 'Special Institutions, Hospitals & Remotes' , 'special' FROM #{$elec}_districts UNION SELECT district_id , 'AV' , 'Absent Votes' , 'special' FROM #{$elec}_districts UNION SELECT district_id , 'PPV' , 'Early Votes (In Person)' , 'special' FROM #{$elec}_districts;"
	puts sql
	sql_upload(sql)

	puts "Special booths done."
end

def create_results_table
	begin
		sql_upload("select * from #{$elec}_results;").first
	rescue
		sql = "CREATE TABLE #{$elec}_results ( `id` int(11) NOT NULL AUTO_INCREMENT, `district_id` varchar(5) DEFAULT NULL, `booth_id` varchar(5) DEFAULT NULL, `candidate_id` int(4) DEFAULT NULL, `vote_type` int(4) DEFAULT NULL, `votes` int(4) DEFAULT NULL, last_updated datetime,
		PRIMARY KEY (`id`), UNIQUE KEY `idx_unique` (`district_id`,`booth_id`,`candidate_id`,`vote_type`), KEY `idx_dcv` (`district_id`,`candidate_id`,`vote_type`), KEY `idx_bcv` (`booth_id`,`candidate_id`,`vote_type`), KEY `idx_cv` (`candidate_id`,`vote_type`), KEY `idx_v` (`vote_type`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
		sql_upload(sql)
	end
end

def create_votetypes_table
	begin
		sql_upload("select * from #{$elec}_vote_types;").first
	rescue
		sql = "CREATE TABLE #{$elec}_vote_types (`vote_type` int(11) NOT NULL AUTO_INCREMENT,`type` varchar(15) DEFAULT NULL, `description` varchar(60) DEFAULT NULL, PRIMARY KEY (`vote_type`), UNIQUE KEY `idx_type` (`type`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
		sql_upload(sql)
		sql_upload("INSERT INTO #{$elec}_vote_types (`vote_type`, `type`, `description`) VALUES ('1', 'primary', 'primary votes for polling place'),('2', 'tcp', 'two candidate preferred for polling place');")
	end
end

def process_primaries(doc)
	@doc = doc

	puts "Processing the primary results"
	sql = "INSERT INTO #{$elec}_results (district_id, booth_id, candidate_id, vote_type, votes, last_updated) VALUES "
	tsql = ""

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		booth = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAPC']/OrdinaryPollingPlaceVotes")
		booth.each do | bth |
			boothcand = bth.xpath("./CandidateVotes")
			boothcand.each do | cand |
				prim = cand['Votes']
				tsql << "(\"#{dis['Code']}\",#{bth['OrdinaryPollingPlaceNumber']},#{cand['CandidateBallotPaperOrder']},1,#{prim},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
			end
		end
		special = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAPC']/CategoryVotes")
		special.each do | bth |
			boothcand = bth.xpath("./CandidateVotes")
			boothcand.each do | cand |
				prim = cand['Votes']
				tsql << "(\"#{dis['Code']}\",\"#{bth['CategoryCode']}\",#{cand['CandidateBallotPaperOrder']},1,#{prim},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
			end
		end
	end
	if tsql == ""
		puts "No Primary results"
	else
		sql << tsql
		sql = sql[0..-2]	#chop off the last comma
		sql << " ON DUPLICATE KEY UPDATE votes = values(votes), last_updated = values(last_updated);"
		sql_upload(sql)
		puts "Primary complete."
	end
end


def process_informals(doc)
	@doc = doc

	puts "Processing the informal votes"
	sql = "INSERT INTO #{$elec}_results (district_id, booth_id, candidate_id, vote_type, votes, last_updated) VALUES "
	tsql = ""

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		booth = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAPC']/OrdinaryPollingPlaceVotes")
		booth.each do | bth |
			tsql << "(\"#{dis['Code']}\",#{bth['OrdinaryPollingPlaceNumber']},98,1,#{bth['InformalVotes']},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
		end
		special = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAPC']/CategoryVotes")
		special.each do | bth |
			tsql << "(\"#{dis['Code']}\",\"#{bth['CategoryCode']}\",98,1,#{bth['InformalVotes']},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
		end		
	end
	if tsql == ""
		puts "No Informal Results"
	else
		sql << tsql
		sql = sql[0..-2]	#chop off the last comma
		sql << " ON DUPLICATE KEY UPDATE votes = values(votes), last_updated = values(last_updated);"
		sql_upload(sql)
		puts "Informals complete."
	end
end


def process_tcp(doc)
	@doc = doc

	puts "Processing the tcp results"
	sql = "INSERT INTO #{$elec}_results (district_id, booth_id, candidate_id, vote_type, votes, last_updated) VALUES "
	tsql = ""

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		booth = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAND']/OrdinaryPollingPlaceVotes")
		booth.each do | bth |
			boothcand = bth.xpath("./CandidateVotes")
			boothcand.each do | cand |
				tcp = cand['Votes']
				if tcp.nil? || tcp.empty? || tcp == 0
					tcp = "NULL"
				else
					tcp = tcp
				end
				tsql << "(\"#{dis['Code']}\",#{bth['OrdinaryPollingPlaceNumber']},#{cand['CandidateBallotPaperOrder']},2,#{tcp},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
			end
		end
		special = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAND']/CategoryVotes")
		special.each do | bth |
			boothcand = bth.xpath("./CandidateVotes")
			boothcand.each do | cand |
				tcp = cand['Votes']
				if tcp.nil? || tcp.empty? || tcp == 0
					tcp = "NULL"
				else
					tcp = tcp
				end
				tsql << "(\"#{dis['Code']}\",\"#{bth['CategoryCode']}\",#{cand['CandidateBallotPaperOrder']},2,#{tcp},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
			end
		end			
	end
	if tsql == ""
		puts "No TCP Results"
	else
		sql << tsql
		sql = sql[0..-2]	#chop off the last comma
		sql << " ON DUPLICATE KEY UPDATE votes = values(votes), last_updated = values(last_updated);"
		sql_upload(sql)
		puts "TCP complete."
	end
end

def process_exhaust(doc)
	@doc = doc

	puts "Processing the exhausted votes"
	sql = "INSERT INTO #{$elec}_results (district_id, booth_id, candidate_id, vote_type, votes, last_updated) VALUES "
	tsql = ""

	district = @doc.xpath("//ElectionRegion/ElectionDistrict")
	district.each do | dis |
		booth = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAND']/OrdinaryPollingPlaceVotes")
		booth.each do | bth |
			exh = bth['ExhaustedVotes']
			if exh.nil? || exh.empty?
				exh = "NULL"
			else
				exh = exh
			end
			tsql << "(\"#{dis['Code']}\",#{bth['OrdinaryPollingPlaceNumber']},99,2,#{exh},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
		end
		special = dis.xpath("./LA/DistrictVotes[@CountDefinitionCode='LAND']/CategoryVotes")
		special.each do | bth |
			exh = bth['ExhaustedVotes']
			if exh.nil? || exh.empty?
				exh = "NULL"
			else
				exh = exh
			end
			tsql << "(\"#{dis['Code']}\",\"#{bth['CategoryCode']}\",99,2,#{exh},str_to_date(\"#{bth['LastUpdated']}\",'%Y-%m-%dT%H:%i:%s')),"
		end			
	end
	if tsql == ""
		puts "No Exhaust Results"
	else
		sql << tsql
		sql = sql[0..-2]	#chop off the last comma
		sql << " ON DUPLICATE KEY UPDATE votes = values(votes), last_updated = values(last_updated);"
		sql_upload(sql)
		puts "Exhaust complete."
	end
end

def execute_merge
	puts "Executing the merge of XML into results"
	sql = "CALL xml_to_results;"
	WADatabase.runQuery('election_night',sql)
	puts "Function complete"
end