Search.setIndex({docnames:["architecture","base_classes/extractor","base_classes/loader","base_classes/overview","base_classes/spooq2_logger","base_classes/transformer","changelog","examples","extractor/jdbc","extractor/json","extractor/overview","index","installation","loader/hive_loader","loader/overview","pipeline/overview","pipeline/pipeline","pipeline/pipeline_factory","setup_development_testing","transformer/enum_cleaner","transformer/exploder","transformer/flattener","transformer/mapper","transformer/newest_by_group","transformer/overview","transformer/sieve","transformer/threshold_cleaner"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":3,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":2,"sphinx.domains.rst":2,"sphinx.domains.std":1,"sphinx.ext.intersphinx":1,"sphinx.ext.todo":2,"sphinx.ext.viewcode":1,sphinx:56},filenames:["architecture.rst","base_classes/extractor.rst","base_classes/loader.rst","base_classes/overview.rst","base_classes/spooq2_logger.rst","base_classes/transformer.rst","changelog.rst","examples.rst","extractor/jdbc.rst","extractor/json.rst","extractor/overview.rst","index.rst","installation.rst","loader/hive_loader.rst","loader/overview.rst","pipeline/overview.rst","pipeline/pipeline.rst","pipeline/pipeline_factory.rst","setup_development_testing.rst","transformer/enum_cleaner.rst","transformer/exploder.rst","transformer/flattener.rst","transformer/mapper.rst","transformer/newest_by_group.rst","transformer/overview.rst","transformer/sieve.rst","transformer/threshold_cleaner.rst"],objects:{"spooq2.extractor":{extractor:[10,0,0,"-"],jdbc:[8,0,0,"-"],json_files:[9,0,0,"-"]},"spooq2.extractor.extractor":{Extractor:[10,1,1,""]},"spooq2.extractor.extractor.Extractor":{extract:[10,2,1,""],logger:[10,3,1,""],name:[10,3,1,""]},"spooq2.extractor.jdbc":{JDBCExtractor:[8,1,1,""],JDBCExtractorFullLoad:[8,1,1,""],JDBCExtractorIncremental:[8,1,1,""]},"spooq2.extractor.jdbc.JDBCExtractorFullLoad":{extract:[8,2,1,""]},"spooq2.extractor.jdbc.JDBCExtractorIncremental":{extract:[8,2,1,""]},"spooq2.extractor.json_files":{JSONExtractor:[9,1,1,""]},"spooq2.extractor.json_files.JSONExtractor":{extract:[9,2,1,""]},"spooq2.loader":{hive_loader:[13,0,0,"-"],loader:[14,0,0,"-"]},"spooq2.loader.hive_loader":{HiveLoader:[13,1,1,""]},"spooq2.loader.hive_loader.HiveLoader":{load:[13,2,1,""]},"spooq2.pipeline":{factory:[17,0,0,"-"],pipeline:[16,0,0,"-"]},"spooq2.pipeline.factory":{PipelineFactory:[17,1,1,""]},"spooq2.pipeline.factory.PipelineFactory":{execute:[17,2,1,""],get_metadata:[17,2,1,""],get_pipeline:[17,2,1,""],url:[17,3,1,""]},"spooq2.spooq2_logger":{get_logging_level:[4,4,1,""],initialize:[4,4,1,""]},"spooq2.transformer":{enum_cleaner:[19,0,0,"-"],exploder:[20,0,0,"-"],flattener:[21,0,0,"-"],mapper_custom_data_types:[22,0,0,"-"],newest_by_group:[23,0,0,"-"],sieve:[25,0,0,"-"],threshold_cleaner:[26,0,0,"-"],transformer:[24,0,0,"-"]},"spooq2.transformer.enum_cleaner":{EnumCleaner:[19,1,1,""]},"spooq2.transformer.enum_cleaner.EnumCleaner":{transform:[19,2,1,""]},"spooq2.transformer.exploder":{Exploder:[20,1,1,""]},"spooq2.transformer.exploder.Exploder":{transform:[20,2,1,""]},"spooq2.transformer.flattener":{Flattener:[21,1,1,""]},"spooq2.transformer.flattener.Flattener":{export_script:[21,2,1,""],get_script:[21,2,1,""],transform:[21,2,1,""]},"spooq2.transformer.mapper":{Mapper:[22,1,1,""]},"spooq2.transformer.mapper_custom_data_types":{_generate_select_expression_for_IntBoolean:[22,4,1,""],_generate_select_expression_for_IntNull:[22,4,1,""],_generate_select_expression_for_StringBoolean:[22,4,1,""],_generate_select_expression_for_StringNull:[22,4,1,""],_generate_select_expression_for_TimestampMonth:[22,4,1,""],_generate_select_expression_for_as_is:[22,4,1,""],_generate_select_expression_for_extended_string_to_boolean:[22,4,1,""],_generate_select_expression_for_extended_string_to_date:[22,4,1,""],_generate_select_expression_for_extended_string_to_double:[22,4,1,""],_generate_select_expression_for_extended_string_to_float:[22,4,1,""],_generate_select_expression_for_extended_string_to_int:[22,4,1,""],_generate_select_expression_for_extended_string_to_long:[22,4,1,""],_generate_select_expression_for_extended_string_to_timestamp:[22,4,1,""],_generate_select_expression_for_extended_string_unix_timestamp_ms_to_date:[22,4,1,""],_generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp:[22,4,1,""],_generate_select_expression_for_has_value:[22,4,1,""],_generate_select_expression_for_json_string:[22,4,1,""],_generate_select_expression_for_keep:[22,4,1,""],_generate_select_expression_for_meters_to_cm:[22,4,1,""],_generate_select_expression_for_no_change:[22,4,1,""],_generate_select_expression_for_timestamp_ms_to_ms:[22,4,1,""],_generate_select_expression_for_timestamp_ms_to_s:[22,4,1,""],_generate_select_expression_for_timestamp_s_to_ms:[22,4,1,""],_generate_select_expression_for_timestamp_s_to_s:[22,4,1,""],_generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp:[22,4,1,""],_generate_select_expression_without_casting:[22,4,1,""],_get_select_expression_for_custom_type:[22,4,1,""],add_custom_data_type:[22,4,1,""]},"spooq2.transformer.newest_by_group":{NewestByGroup:[23,1,1,""]},"spooq2.transformer.newest_by_group.NewestByGroup":{transform:[23,2,1,""]},"spooq2.transformer.sieve":{Sieve:[25,1,1,""]},"spooq2.transformer.sieve.Sieve":{transform:[25,2,1,""]},"spooq2.transformer.threshold_cleaner":{ThresholdCleaner:[26,1,1,""]},"spooq2.transformer.threshold_cleaner.ThresholdCleaner":{transform:[26,2,1,""]},spooq2:{spooq2_logger:[4,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","function","Python function"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:function"},terms:{"0000":22,"00am":22,"0x7f5dc8eb2890":4,"100":22,"117":21,"11t00":22,"123456":22,"123_456":22,"12t12":22,"143":21,"154":7,"1547204429":7,"1547204429000":7,"1547371284":7,"1580737513":[7,26],"1581540839":22,"1581540839000":22,"1591627696951":22,"1596812952000":22,"1597069446":22,"1597069446000":22,"165":22,"16707":22,"16t00":22,"16t22":7,"17429":22,"17484":22,"180":22,"18994":7,"1970":22,"1972":7,"1988":22,"1999":22,"1st":22,"200":13,"2018":17,"2019":[11,22],"2020":[4,8,11,22],"20200201":[2,7,8,9,13],"2021":[11,21],"205":22,"2095":22,"2099":22,"2120":22,"21474836470":22,"21474838464":22,"21_474_836_470":22,"21_474_838_464":22,"237":7,"250":26,"253":4,"3047288":22,"312":1,"313":4,"3391":7,"3974400000":22,"3993":22,"4057":7,"41z":7,"469497005c1f":7,"4737139200":22,"4737139200000":22,"4789":21,"478934243342334":21,"4887839":22,"4887839000":22,"5000":17,"53pm":22,"5432":8,"57815":22,"5c78":7,"819":22,"836470":22,"836_470":22,"870":21,"921":21,"942":21,"946672200000":22,"951000":22,"9637":7,"9939":7,"9952":7,"ad\u00e9la\u00efd":22,"ana\u00ebl":7,"b\u00e9rang\u00e8r":7,"boolean":[22,25],"break":6,"case":[6,17,22],"class":[3,8,9,11,13,17,19,20,21,23,25,26],"dani\u00e8l":7,"daphn\u00e9":22,"default":[2,5,6,7,8,13,17,18,19,20,21,22,23,26],"export":21,"final":9,"function":[2,6,21,22,23,26],"import":[1,2,4,5,6,7,8,9,12,18,20,21,22],"int":[2,8,9,13,22],"long":21,"new":[1,2,5,6,18,21,22],"no\u00e9mi":7,"null":[5,6,7,19,22],"public":[8,9],"return":[1,2,4,5,8,9,10,17,19,20,21,22,23,24,25,26],"ru\u00f2":22,"super":[1,2,5],"true":[1,8,9,13,19,20,21,22],"try":21,"while":[1,2,5],ARE:19,But:18,For:[4,8,12,18,22],Has:13,NOT:[19,22],Not:13,One:[1,2,5],That:[1,2,5],The:[1,2,4,5,8,9,12,13,17,18,19,20,22,23,25,26],There:[17,18,22],These:[17,22],Used:22,__all__:[1,2,5],__init__:[1,2,4,5],__name__:10,__version__:12,_at:21,_construct_query_for_partit:8,_date:21,_explode_and_get_map:21,_generate_select_expression_for_as_i:22,_generate_select_expression_for_extended_string_to_boolean:22,_generate_select_expression_for_extended_string_to_d:22,_generate_select_expression_for_extended_string_to_doubl:22,_generate_select_expression_for_extended_string_to_float:22,_generate_select_expression_for_extended_string_to_int:22,_generate_select_expression_for_extended_string_to_long:22,_generate_select_expression_for_extended_string_to_timestamp:22,_generate_select_expression_for_extended_string_unix_timestamp_ms_to_d:22,_generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp:22,_generate_select_expression_for_has_valu:22,_generate_select_expression_for_intboolean:22,_generate_select_expression_for_intnul:22,_generate_select_expression_for_json_str:22,_generate_select_expression_for_keep:22,_generate_select_expression_for_meters_to_cm:22,_generate_select_expression_for_no_chang:22,_generate_select_expression_for_stringboolean:22,_generate_select_expression_for_stringnul:22,_generate_select_expression_for_timestamp_ms_to_:22,_generate_select_expression_for_timestamp_ms_to_m:22,_generate_select_expression_for_timestamp_s_to_:22,_generate_select_expression_for_timestamp_s_to_m:22,_generate_select_expression_for_timestampmonth:22,_generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp:22,_generate_select_expression_without_cast:22,_get_select_expression_for_custom_typ:22,_time:21,_to_hello_world:22,_version:12,a998:7,abl:[4,6,22],about:[17,22],access:20,accordingli:[5,9,24],across:6,action:[7,8],activ:[7,11,14,19,24],actual:[1,8,22],actual_count:1,ad_hoc:17,adapt:[1,2,5,17],add:[1,2,5,6,18,22],add_custom_data_typ:22,add_transform:7,added:[6,9,22],addfil:12,addit:6,addition:22,advis:22,affect:[4,22],aforement:16,after:[6,8],afterward:20,again:8,age:22,alia:22,alias:24,all:[1,2,5,6,8,9,10,13,18,19,20,21,22,23,25,26],all_friend:13,allow:[2,6,18,19],alreadi:[2,13,18,22],also:[2,4,5,7,13,18,22],although:[18,20,22],ambigu:22,ani:[1,2,5,8,10,17,19,20,22,23],anonym:22,anymor:6,anyth:2,apach:8,api:[2,8,9],append:[6,22],appli:[5,17,21,22,24],applic:[11,12,18],appnam:1,arbitrari:22,architectur:[11,18],argument:[5,23,24],around:20,arrai:[5,6,20,22,24],as_i:[7,24],asctim:4,ask:17,assert:[1,2,5],assertionerror:[2,8,13],assum:22,attent:[18,22],attribut:[1,2,5,7,19,20,22,23,25],attributeerror:[9,23],authent:8,author:17,auto_create_t:13,autoclass:[1,2,5],automat:[1,2,5,17,18],automodul:[1,2,5],avail:[11,24],avoid:[7,18,22],b12b59ba:7,base:[10,11,13,17,22,24],base_path:9,basestr:2,basi:13,basic:[5,24],batch:22,batch_siz:17,bdist_egg:12,becaus:19,been:7,befor:13,begin:22,behav:22,being:20,better:22,between:22,bewar:18,bin:[6,18],birthdai:[7,22],bool:[8,13,20,21,22],booleantyp:22,both:11,boundari:8,breaka84:[6,12],breakpoint:18,broke:6,bug:22,build:[1,2,5,8,11,17,18],builder:1,built:[6,22],busi:17,bypass:7,cach:[7,8],call:[1,2,5,8,9,17,18,22],can:[1,2,5,6,11,14,17,20,21,22,24],cannot:20,capabl:17,care:22,cast:[6,22],chain:16,chang:[6,18],changelog:11,check:9,choic:18,chromium:18,class_diagram:[1,5],clean:[9,17,19,22,26],cleaner:[6,11,24],cleaning_definit:19,cleans:[5,19,24],clear:13,clear_partit:13,close:18,code:[3,11,18,21],codec:2,col:[5,22],col_nam:26,col_typ:26,collect:[17,22],column:[2,4,5,6,8,13,19,20,21,22,23,26],column_nam:[7,13,19],column_typ:[7,13],com:[6,7,12,22],combin:9,command:18,comment:18,common:7,common_df:7,commonmark:18,compar:22,compat:[18,22],complex:[6,9,17,21,22],compli:[17,25],compon:[11,17],compress:2,compression_codec:2,comput:18,concat_w:22,condit:25,conf:18,configur:[1,2,4,5,8,11,17],connect:8,consequ:17,consid:[5,22],construct:[17,22],constructor:22,contain:[1,2,5,13,17,19,20,21,22,23,26],content:[13,18],context:17,context_vari:17,control:22,convers:[6,22],convert:[1,5,8,10,22],convert_timestamp:21,correctli:22,could:[7,22],count:[1,2,5],cov_html:18,coverag:18,creat:[3,6,11,13,20],created_at:[7,8,22,26],created_at_m:[7,23],created_at_sec:7,created_on:22,createdatafram:[21,22],csv:1,csv_extractor:1,csvextractor:1,current:[8,9,11,17,18,21,24],current_d:[22,26],current_timestamp:22,custom:[6,11,24],custom_typ:22,cut:12,dai:22,daili:[9,17],daphn:22,data:[1,2,5,6,8,9,10,11,13,16,17,22,26],data_typ:22,databas:[1,8,10,11,14,17],datafram:[1,2,5,6,7,8,9,10,13,14,17,19,20,21,22,23,24,25,26],dataframeread:[1,8],dataframewrit:2,datatyp:[13,22],date:[5,17,22,24,26],datetim:[21,22],datetyp:[22,26],db_name:[7,13],dbf:[1,10],dbname:8,deactiv:18,debug:4,debugg:18,decim:22,decor:6,decreas:17,deepest:21,def:[1,2,5,22],default_extractor:1,default_load:2,default_transform:5,default_valu:[7,13],defin:[4,6,8,9,10,13,17,18,19,20,21,22,23,25,26],delet:13,deleted_at:23,depend:[1,4,18,20,21,25],deploy:11,deriv:[8,9],descend:23,describ:17,descript:18,deseri:9,design:18,destin:13,detail:[10,11,14,24],dev:[4,18],develop:11,diagram:[1,2,5,11],dict:[8,13,17,19,26],dictionari:[13,19,26],did:19,differ:[6,18],direct:[1,2,5,18],directli:[2,5,7,11,21,22],disabl:[6,18],disallow:19,discov:18,dist:12,distinguish:22,distribut:18,do_some_stuff:1,doc:[1,2,5,8,18],docstr:[1,2,5,18],document:[3,17],docutil:18,doe:[2,8,10,13,17,19,20,21,22,23,25,26],don:[6,20],done:[1,2,5,6],dot:20,doubl:[21,22],double_v:21,doubletyp:22,download:18,driver:8,drop:[5,7,20],drop_rows_with_empty_arrai:[6,20],dropna:5,dropper:5,dynam:[6,13,22],each:[2,5,14,17,24,26],eachtim:8,effect:13,egg:11,either:[9,13,18],elem1:19,elem2:19,elem:[20,22],element:[19,20,23],elif:[2,9],els:[4,9,22],email:[7,22],empti:[6,19,22],enabl:[6,13,18],enablehivesupport:1,encourag:22,end:[16,17,22],endpoint:17,engin:17,enter:18,entity_typ:17,enumclean:[6,19],enumer:[11,24],environ:[4,11],equal:[20,25],error:[4,18,22],especi:22,essenti:[10,22],etl:[11,17],eval:6,evalu:22,evinc:18,exampl:[1,2,4,5,8,9,11,13,17,18,19,20,21,22,23,25,26],except:[1,2,5,8,13,19,22,23,25,26],execut:[7,17,18,21],exemplari:[3,10],exist:[2,13,22],exit:18,expect:[1,2,5,13],expected_count:1,experiment:17,expert:17,explicit:[1,22],explicit_partition_valu:2,explicitli:[13,19,21],explod:[5,6,7,11,21,24],explode_out:20,exploded_elem_nam:[7,20],explos:[6,21],export_script:21,express:[21,22,25],ext4:[1,10],extended_str:6,extended_string_to_boolean:[6,24],extended_string_to_d:[6,24],extended_string_to_doubl:[6,24],extended_string_to_float:[6,24],extended_string_to_int:[6,24],extended_string_to_long:[6,24],extended_string_to_timestamp:[6,24],extended_string_unix_timestamp_ms_to_d:[6,24],extended_string_unix_timestamp_ms_to_timestamp:[6,24],extern:[18,21],extract:[1,2,5,7,8,9,10,16,22],extracted_df:[1,8],extractor:[3,6,7,8,9,11,17,18],facebook_id:22,factori:[11,15],fail:[6,18],fairli:18,fallback:6,fals:[6,8,13,19,20,21,22],featur:[1,2,5,18],feel:22,fetch:[1,8,10,17],field:20,file:[1,2,5,10,11,13,18,21],file_nam:21,fill:2,fillna:19,filter:[5,11,17,24],filter_express:25,find:19,fine:4,first:[20,22,23],first_and_last_nam:22,first_nam:[5,7,8,22,23],fix:6,fixtur:[1,2,5],flag:[6,13,21,22],flat:22,flat_df:21,flatten:[6,11,24],flattenend:21,flexibl:7,floattyp:22,flow:11,folder:[6,18],follow:[17,18,19,22],food:22,forenam:7,form:[8,9],format:[1,4,5,9,18],forward:21,forwared:[],found:18,framework:18,friend:[7,20,22],friend_id:7,friends_df:7,friends_json:22,friends_map:11,friends_partit:13,friends_pipelin:7,from:[1,2,5,6,7,8,9,10,11,13,16,19,21,22],from_thesi:[1,5],full_nam:22,fulli:[17,22],func:22,funcnam:4,function_nam:22,further:[10,14,24],futur:8,gender:[2,7,8,25],gener:[11,21],get:[17,18,19,20,21,22],get_logging_level:4,get_metadata:17,get_pipelin:17,get_script:21,getlogg:4,getorcr:1,gisaksen4:22,git:11,github:[6,12],give:22,glob:1,global:[1,2,3,5,11],glue:16,googl:18,gpirri3j:7,grain:4,graph:18,grep:12,groom:17,group:[4,11,24],group_bi:[7,23],guarante:[20,25],guid:7,gzip:2,handl:[6,22],happen:22,has:[1,2,4,5,8,12,13,14,20,22,24,25],has_email:7,has_univers:7,has_valu:[6,24],hasattr:[1,2,5],have:[2,4,5,7,13,14,17,18,20,22,24],hbase:[2,14],hdf:[1,10],head:22,header:1,hello:[4,21,22],hello_from:22,hello_who:22,hello_world:22,helper:11,here:[1,2,5],hierarchi:22,highli:22,hive:[2,8,11,14,17,22],hive_load:2,hiveload:[2,7,13],hoc:22,how:[1,2,5,18,20,22],html:[1,2,5,8,11],http:[6,8,12,17],huge:18,id_column:5,id_funct:6,idea:18,identifi:5,ignor:[19,22],ignore_ambiguous_column:[6,21,22],ignore_missing_column:[6,22],imag:18,implement:[1,2,5,17,18],implicit:22,inact:19,includ:[3,11,13,20],incom:[5,24],increment:8,independ:11,index:[11,18],indic:22,inf:22,infer:[8,9,17,22],inferschema:1,info:[1,2,4,5,21],inform:[8,22],ingest:11,inherit:[1,2,5],ini:18,init:9,initi:[1,2,4,5,8,10,13,19,20,21,23,25,26],inject:[6,22],inlin:18,input:[1,2,4,5,6,8,9,10,11,13,14,19,20,21,22,23,24,25,26],input_data:1,input_df:[2,5,7,13,19,20,21,22,23,25,26],input_fil:1,input_kei:22,input_path:[1,7,9],input_str:22,insert:13,insid:18,instal:[11,18],instanc:[1,2,4,5,6,10,11,17],instanti:6,instead:[2,5,13,18,21,22],int_val:21,intboolean:24,integ:22,integertyp:[7,13,22],interact:18,interest:17,interfac:17,intern:[1,2,5,20,21,22,23,25],interpret:22,intersphinx_map:18,interv:22,intnul:24,invalid:22,ip_address:7,ipython:18,isin:19,isinst:2,item:13,its:[18,20,22],itself:[17,22],java:[18,22],jdbc:[1,2,10,11,14],jdbc_option:8,jdbcextractor:8,jdbcextractorfullload:[1,8],jdbcextractorincrement:[1,8],jdk8:18,jeannett:7,json:[1,10,11,17,22],json_fil:1,json_str:[6,24],jsonextractor:[1,7,9],just:[1,2,5,18,20,22],keep:[6,24],keep_original_column:21,kei:[17,21,22],kept:[21,22,25],know:20,kudu:[2,14],lake:11,last_dai:17,last_nam:[5,7,8,22,23,25],latex:18,latexpdf:18,launch:12,least:[2,4],led:7,len:2,length:2,let:[1,2,5,22],level:[4,6,10,13,20,22],levelnam:4,lib:6,librari:[11,12],like:[1,2,5,10,22],line:[9,21],lineno:4,link:18,linux:18,list:[1,2,5,13,19,21,22,23],lit:[2,19,22],live:18,load:[1,2,7,8,9,13,14,16,17],loaded_df:2,loader:[3,7,11,13,17,18],local:[11,18,21],localhost:[8,17],locat:18,lock:6,log:[4,6,8,10],logga:4,logger:[1,2,3,5,6,10,11],logging_exampl:4,loghlen:7,logic:22,long_val:21,longer:6,longtyp:[6,22],look:22,lower:[6,25],lyddiard:22,main:[18,22],make:[2,5,18],mandatori:[1,2,5,19],manjaro:18,map:[5,6,7,11,17,21,24],mapper:[5,6,7,11,21,24],mapper_custom_data_typ:22,mapper_custom_data_types_fixtur:22,markdown:18,master:12,max:[7,26],max_timestamp_:22,mean:[20,22],meant:22,member:22,messag:4,meta:7,metadata:17,meter:22,meters_to_cm:[6,24],meth:[1,2,5],method:[1,2,5,6,8,9,10,11,13,14,19,20,21,23,24,25,26],microsoft:8,milli:[6,22],millisecond:[6,22],min:[7,26],minimum:[1,2,5],miss:[5,6,9,22],mktemp:2,mod:6,mode:[19,22],modul:[2,4,11,22],month:22,more:[6,8,18,22],most:[5,11,18,24],multipl:[1,2,5,22],must:[2,13],my_friend:22,name:[1,2,4,5,6,8,10,13,17,19,20,21,22,26],napoleon:[1,2,5],necessari:[8,17,22],need:[1,2,4,5,6,8,10,13,18,19,20,21,22,23,25,26],neg:22,neither:19,nest:[6,20,22],newest:[11,24],newest_by_group:[4,5],newestbygroup:[5,7,23],newli:[1,2,5],next:[21,22],no_chang:24,no_id_dropp:5,noiddropp:5,non:[6,19,22],none:[2,5,6,7,9,13,17,19,22,26],nor:19,normal:13,notat:20,note:22,noth:2,now:6,nullabl:[21,22],number:[2,13,18,22],numer:26,numpi:[1,2,5,18],object:[1,2,5,6,7,8,10,13,16,17,19,20,21,22,23,25,26],obscur:22,occur:22,off:6,omit:9,onc:11,onli:[1,2,5,9,13,17,19,20,21,22,23,25,26],only_sparkx:6,open:[18,21],openjdk:18,option:8,oracl:7,order:23,order_bi:[7,23],org:8,origin:[1,2,5,21],original_column:21,other:[13,18,19,20,21,23,25,26],otherwis:[8,13,22],out:[17,18],outier:26,output:[2,4,11,12,13,20,22],output_df:[1,2,5,7,22],output_path:2,output_valu:22,outsid:6,over:22,overview:[1,2,5,11,18],overwrit:2,overwrite_partition_valu:13,own:[3,11,22],packag:[6,11,18],page:[1,2,5,11],param:17,paramet:[1,2,4,5,6,8,9,10,13,17,19,20,21,22,23,25,26],parameter:[1,2,5],paremt:[2,14],parquet:[2,5],parquet_fil:2,parquet_output:2,parquetfil:[2,5,14],parquetload:2,pars:[1,2,5,18],particular:22,partit:[2,8,9,11,13,17],partition_bi:2,partition_definit:[7,13],partitionbi:2,pass:[17,22],passthrough:6,password:8,path:[1,2,9,20,21,22],path_to_arrai:[7,20],pdf:[1,2,5,11],per:[11,13,18,19,22,24],perform:[2,8,19,20,21,23,25,26],persist:[2,13,17],pip:12,pipelin:[1,2,5,7,11],pipeline_factori:17,pipelinefactori:17,pipenv:18,pipenv_venv_in_project:18,pipfil:18,pivot:22,place:18,plant:18,pleas:[1,2,5,9,10,12,14,17,18,22,24],plug:[1,2,5,11],plugin:11,point:[17,22],posit:22,possibl:[2,5,14,16,21,22,24],post:17,postgresql:8,pre:[4,11],preced:22,preceed:22,prepend:[6,22],prerequisit:11,present:8,pretty_nam:21,previou:8,previous:8,primit:19,printschema:[21,22],probabl:18,process:[7,16],process_d:22,produc:[4,18],product:18,project:18,proper:[1,2,5],provid:[1,2,5,7,8,9,17,19,22,24,25,26],pull:6,puml:[1,5,18],purchas:8,py2:12,pyspark:[1,2,5,6,8,9,10,11,12,13,14,17,18,19,20,21,22,23,24,25,26],pytest:[1,2,5,6,18],python2:18,python:[4,6,12,18,19,21,22],queri:[8,17,22],rais:[1,2,5,8,9,13,19,22,23,25,26],rang:[22,26],range_definit:7,raw:1,rdd:1,read:[1,2,5,18,22],read_onli:8,reader:1,readi:17,reason:17,receiv:4,record:[2,5,11,24,25],redefin:20,redund:7,refactor:6,refer:[3,12,18],referenc:[20,22],regist:22,reject:19,relat:[1,10],relationship:22,relev:17,reload:[7,8],remov:[6,18],renam:22,render:18,repartit:13,repartition_s:[7,13],replac:[19,22],report:18,repositori:11,represent:[1,2,5,22],request:17,requir:[17,18,19,22],respect:[8,19,26],respond:17,respons:17,result:[13,18,20,21,22,25],reus:[1,2,5],right:22,rightmost:21,rlike:25,robust:22,root:[4,6,18,21,22],row:[6,20,21,22,23,25],row_numb:23,rst:[1,2,5,18],rule:[17,19],run:[11,12],runtim:22,same:[1,2,7,18,22],sampl:[3,11,22],save:[2,14],schema:[1,2,5,6,22],schema_v1:[2,5,7,9],scope:2,script:[7,21],search:11,second:[6,8,22],see:[6,8,10,14,17,18,21,22,24],seed:18,select:[5,8,21,22,23,24],selectnewestbygroup:6,self:[1,2,5],send:17,sent:8,sep:1,separ:[11,18,22],sequencefil:[7,9],server:8,servic:17,session:12,set:[1,2,5,6,7,8,9,10,11,13,17,19,20,22,26],set_extractor:7,set_load:7,set_trac:18,setup:[11,12],sever:6,share:[6,7,10,18],shell:18,shortest:21,should:[1,2,5,7,9,13,17,19,21,22],show:[7,22],shuffl:18,siev:[5,11,24],simpl:[1,2,5],simpli:18,simplifi:[1,2,5,11],singl:[2,9,13,14],sink:[2,14],size:[18,20,22,25],size_cm:26,size_in_cm:22,size_in_m:22,skip:22,skype:22,smaller:22,snappi:2,some:[1,2,5,6,21,22],some_attribut:22,some_extractor_inst:[2,5],some_transformer_inst:2,someth:4,sort:23,sourc:[1,2,4,5,9,10,11,13,17,18,19,20,21,22,23,25,26],source_column:22,source_t:8,spark3:18,spark:[1,6,8,11,13,19,20,21,22,25],spark_hom:18,spark_sess:[1,2,5],spark_timestamp:22,sparksess:[1,2,5],special:21,specif:[8,20,21,22],specifi:22,sphinx:[1,2,5,18],sphinxcontrib:18,spooq2:[1,2,4,5,7,8,9,11,18,21,22],spooq2_:12,spooq2_jdbc_log_us:8,spooq2_valu:8,spooq2_values_db:8,spooq2_values_partition_column:8,spooq2_values_t:8,spooq:[4,6,9,12,17,18,19,20,21],spooq_env:4,spooq_rul:17,sql:[1,2,5,6,8,9,10,13,14,17,19,20,21,22,23,24,25,26],src:[1,2,5,12],start:[16,18,21],statement:22,statu:19,std_err:6,std_out:6,stderr:[4,6],stdout:4,step:[7,21],still:[12,17,20,22],store:[2,8,9,12,18],str:[1,2,4,5,8,9,10,13,17,19,20,21,22,23,25],straightforward:18,string:[1,2,5,6,8,9,13,21,22,25],string_val:21,stringboolean:[7,24],stringnul:24,stringtyp:[7,13,22],struct:[20,21,22],struct_val_1:21,struct_val_2:21,struct_val_3:21,struct_val_4:21,structur:[5,17,20,22,24],style:[1,2,5,18],sub:[9,18],submit:12,subnam:8,subpackag:[1,2,5,11],subprotocol:8,subset:5,subsystem:18,suffici:[1,2,5],suffix:21,superclass:[1,2,5],suppli:17,support:[1,6,7,9,13,18,19,20,22,26],surenam:7,surnam:7,surpress:22,svg:18,symlink:18,symplic:18,system:[1,8,10,17],tabl:[2,8,13,14],table_nam:[7,13],tablenam:8,take:[1,2,5,8,10,13,14,19,20,21,22,23,24,25,26],talk:22,tall:22,target:13,tell:13,temp:12,temporarili:18,term:18,termin:18,test123:8,test:[3,4,6,7,9,11,22],test_count:1,test_count_did_not_chang:2,test_csv:1,test_db:8,test_fixtur:22,test_logger_should_be_access:[1,2,5],test_mapper_custom_data_typ:22,test_name_is_set:[1,2,5],test_no_id_dropp:5,test_parquet:2,test_records_are_drop:5,test_schema:1,test_schema_is_unchang:[2,5],test_str_representation_is_correct:[1,2,5],testbasicattribut:[1,2,5],testcsvextract:1,testextendedstringconvers:22,testnoiddropp:5,testparquetload:2,tex:18,texliv:18,text:[1,2,5,22],textfil:9,than:[2,22],thei:[1,2,5,7],them:[9,20],theme:18,therefor:[17,20,22],thi:[1,2,5,7,8,9,10,13,16,18,19,20,21,22,23,25,26],thing:[1,2,5],those:[18,22],thousand:22,three:22,thresh:5,threshold:[5,6,11,24],threshold_clean:5,thresholdclean:[5,6,7,26],tibbl:7,tight:22,time:[1,2,5,6,21,22],time_rang:17,time_sec:22,timestamp:[6,21,22,26],timestamp_ms_to_:[7,24],timestamp_ms_to_m:24,timestamp_s_to_:24,timestamp_s_to_m:24,timestamp_v:21,timestampmonth:24,timestamptyp:[22,26],timezon:22,tini:22,tmpdir_factori:2,toctre:[1,2,5],todo:22,togeth:16,touch:6,trail:22,trank:22,transform:[2,3,6,7,11,16,17,18,19,20,21,22,23,25,26],transformed_df:5,treat:22,tri:9,tsivorn1:22,tupl:[5,22],type1extractor:17,type1load:17,type1transform:17,type2transform:17,type3transform:17,type4transform:17,type5transform:17,type:[4,6,8,9,10,13,16,17,19,20,21,22,23,25,26],typeerror:1,typic:11,typo:22,u00e9:22,u00e9la:22,u00efd:22,u00f2:22,ubuntu:18,udf:[20,22],udf_hello_world:22,uml:[1,5,18],uncov:22,under:[21,22],underscor:22,unicod:[1,2,5,22],unit:[1,2,5,22],univers:7,unix:[6,22],unix_t:22,unix_timestamp_in_m:22,unix_timestamp_ms_to_spark_timestamp:[6,24],unnecessari:7,updat:[6,11,22],updated_at:[8,23],upgrad:6,upon:8,url:[8,17],use:[1,2,5,6,8,17,18,19,20,21,22],used:[1,2,4,7,8,10,11,13,17,18,19,20,22,23,25],useful:[18,22],user:[8,11,17,22],user_id:[4,5],usernam:8,users_and_friend:[7,13],users_map:7,users_pipelin:7,uses:[1,2,5,18,21],utc:22,val:17,valid:[8,13,22,25,26],valu:[2,5,6,8,13,17,18,19,22,26],valueerror:[5,19,25,26],vari:22,variabl:[4,8,17,18],venv:18,verbos:7,version:[6,7,18,19,22,23],version_numb:12,via:[1,2,4,5,6,10,17,22],view:18,virtual:11,virtualenv:18,visit:22,wai:22,want:[18,19,22],weather:[8,22],well:7,when:[2,6,7,22],where:[2,5,8,18,22],whether:[13,21],which:[1,2,5,7,8,9,10,11,13,14,17,18,19,20,21,22,24,25],whitespac:22,who:22,window:[18,23],wisdom:22,withcolumn:2,within:[1,2,5,12,18,20,23,26],without:[5,6,22],without_cast:24,work:[6,18,20],world:[4,22],would:[1,2,5,7,13,22],write:[1,2,5,6,18,20],written:7,wsl2:18,yet:[2,13],you:[1,2,5,17,18,19,20,22],your:[3,11,22],yyyymmdd:9,zero:22,zip:[2,11]},titles:["Architecture Overview","Extractor Base Class","Loader Base Class","Spooq Base","Global Logger","Transformer Base Class","Changelog","Examples","JDBC Source","JSON Files","Extractors","Welcome to Spooq\u2019s documentation!","Installation / Deployment","Hive Database","Loaders","Pipeline","Pipeline","Pipeline Factory","Setup for Development, Testing, Documenting","Enumeration-based Cleaner","Exploder","Flattener","Mapper","Newest by Group (Most current record per ID)","Transformers","Sieve (Filter)","Threshold-based Cleaner"],titleterms:{"2019":6,"2020":6,"2021":6,"class":[0,1,2,5,10,14,15,22,24],activ:[13,18,22],alias:22,applic:7,architectur:0,as_i:22,avail:22,base:[1,2,3,5,19,26],both:7,build:12,changelog:6,cleaner:[19,26],code:[1,2,5,7],compon:18,configur:18,content:11,cov:18,creat:[1,2,5,10,14,18,24],current:23,custom:22,data:[0,7],databas:13,deploy:12,detail:22,develop:[12,18],diagram:[0,10,13,14,15,18,22,24],directli:12,document:[1,2,5,11,12,18],egg:12,enumer:19,environ:18,exampl:7,exemplari:[1,2,5],explod:20,extended_string_to_boolean:22,extended_string_to_d:22,extended_string_to_doubl:22,extended_string_to_float:22,extended_string_to_int:22,extended_string_to_long:22,extended_string_to_timestamp:22,extended_string_unix_timestamp_ms_to_d:22,extended_string_unix_timestamp_ms_to_timestamp:22,extractor:[1,10],factori:17,file:[7,9,12],filter:25,flatten:21,flow:0,friends_map:7,from:12,gener:18,git:12,global:4,group:23,has_valu:22,hive:[7,13],html:18,includ:[1,2,5,12],indic:11,input:7,instal:12,intboolean:22,intersphinx:18,intnul:22,ipdb:18,jdbc:8,json:[7,9],json_str:22,keep:22,loader:[2,14],local:12,logger:4,map:22,mapper:22,meters_to_cm:22,method:22,most:23,napoleon:18,newest:23,no_chang:22,onc:7,order:18,output:7,overview:0,own:[1,2,5,10,14,18,24],packag:12,partit:7,pdf:18,per:23,pipelin:[0,15,16,17],plantuml:18,plugin:18,pre:12,prerequisit:18,random:18,recommonmark:18,record:23,refer:[1,2,5],repositori:12,run:18,sampl:[1,2,5,7],set:18,setup:18,siev:25,simplifi:0,sourc:8,spark:[12,18],spooq2:12,spooq:[0,3,11],stringboolean:22,stringnul:22,subpackag:[10,14,15,24],tabl:[7,11],test:[1,2,5,12,18],threshold:26,timestamp_ms_to_:22,timestamp_ms_to_m:22,timestamp_s_to_:22,timestamp_s_to_m:22,timestampmonth:22,transform:[5,24],typic:0,unix_timestamp_ms_to_spark_timestamp:22,updat:7,user:7,virtual:18,welcom:11,without_cast:22,your:[1,2,5,10,14,18,24],zip:12}})