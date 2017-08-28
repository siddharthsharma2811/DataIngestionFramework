REGISTER '$jar_file';

data = load '$input_dir'
       using org.apache.pig.piggybank.storage.FixedWidthLoader(
       '$pos_param',
       '$header_option',
       '$field_list'
       );

store data into '$output_dir'
      using PigStorage('|');

	  
