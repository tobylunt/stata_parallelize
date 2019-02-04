
/**************************************************************************************************/
/* program gnu_parallelize : put together the parallelization of a section of code in bash    */
/**************************************************************************************************/

/* this program writes a few temp files and calls GNU parallel to run
a program in parallel. assumes your program takes a `group' and a
`directory' option. by: TL */

/* a hypothetical example call of this would be: gnu_parallelize, max_cores(5) program(gen_data.do) \\\
input_txt($tmp/par_info.txt) progloc($tmp) options(group state) maxvar pre_comma diag */

cap prog drop gnu_parallelize
prog def gnu_parallelize
{

  /* progloc is required if the program isn't in the default stata path. */
  syntax , MAX_jobs(real) PROGram(string) [INput_txt(string) options(string) progloc(string) pre_comma rmtxt maxvar DIAGnostics trace tracedepth(real 2) manual_input static_options(string) extract_prog prep_input_file(string)]

  /* create a random number that will serve as our job ID for this random task */
  !shuf -i 1-10000 -n 1 >> $tmp/randnum.txt

  /* read that random number into a stata macro */
  file open myfile using "$tmp/randnum.txt", read
  file read myfile line
  local randnum "`line'"
  file close myfile

  /* remove the temp file */
  rm $tmp/randnum.txt

  /* display our temporary do file location */
  if !mi("`diagnostics'") {
    disp_nice "Writing log and temp dofile to: $tmp/parallelizing_dofile_`randnum'.[do,log]"
  }

  /* initialize a temporary dofile that will run the data generation for
  a single group */
  file open group_dofile using "$tmp/parallelizing_dofile_`randnum'.do", write replace

  /* if we want a more diagnostic log, set trace on */
  if !mi("`trace'") {
  file write group_dofile "set trace on" _n
    file write group_dofile "set tracedepth `tracedepth'" _n
  }

  /* fill out the temp dofile. if we want to prepare the inputs
  (groups) into a text file, do so */
  if !mi("`prep_input_file'") {
    prep_gnu_parallel_input_file $tmp/gnu_parallel_input_file_`randnum'.txt, in(`prep_input_file')
    local input_txt $tmp/gnu_parallel_input_file_`randnum'.txt
  }

  /* if we want to expand maximum number of vars, do so */
  if !mi("`maxvar'") {
    file write group_dofile "clear all" _n
    file write group_dofile "clear mata" _n
    file write group_dofile "set maxvar 30000" _n
    file write group_dofile "qui do ~/iecmerge/include/include.do" _n
  }

  /* load in the program if necessary. */
  if !mi("`progloc'") {

    /* if we want the program to be extracted from a larger do-file,
    then do so */
    if !mi("`extract_prog'") {

      /* use program in include.do to extract program to temp, saving
      in $tmp/tmp_prog_extracted.do */
      extract_collapse_prog `program', progloc("`progloc'") randnum("`randnum'")
      if !mi("`diagnostics'") {
        file write group_dofile "do $tmp/tmp_prog_extracted_`randnum'.do" _n
      }
      else {
        file write group_dofile "qui do $tmp/tmp_prog_extracted_`randnum'.do" _n
      }
    }
    /* if no extraction needed, then use the program location */
    else if mi("`extract_prog'")  {
      if !mi("`diagnostics'") {
        file write group_dofile "do `progloc'" _n
      }
      else {
        file write group_dofile "qui do `progloc'" _n
      }
    }
  }

  /* show our input values (from "options") from the unix shell, if
  diagnostics are turned on. with manual override this won't do
  anything, so no harm done. */  
  if !mi("`diagnostics'") {

    /* if we have no input before the option comma, options start at 1 */
    local option_index 1
      if !mi("`pre_comma'") {
        /* if so, start at 2 */
        local option_index 2
        file write group_dofile "disp "
        file write group_dofile `"`=char(34)'"'
        file write group_dofile "\`1"
        file write group_dofile "'"
        file write group_dofile `"`=char(34)'"' _n
      }
    foreach option in `options' {
      file write group_dofile "disp "
      file write group_dofile `"`=char(34)'"'
      file write group_dofile " \``option_index'"
      file write group_dofile "'"
      file write group_dofile `"`=char(34)'"' _n
      local option_index = `option_index' + 1
    }
  }


  /* check if a manual override has been specified. if so, we need to
  get the complete program call lines in from our manual_override
  .txt, and call them one by one. */
  if !mi("`manual_input'") {

    /* step 1: count the number of program calls we need to make. */
    file open txtlines using `input_txt', read
    local num_lines = 1
    file read txtlines line
    while r(eof)==0 {
      file read txtlines line
      /* check if there's an empty line (somtimes happens at the end -
      assumes no missing lines in the middle*/
      if !mi("`line'") {
        local num_lines = `num_lines' + 1
      }
    }
    file close txtlines
    
    /* step 2: write a sequence, one number per line, of 1:count in a
    separate text file */
    file open index_seq using $tmp/index_sequence_`randnum'.txt, write replace
    forval line = 1/`num_lines' {
      file write index_seq "`line'" _n
    }
    file close index_seq
    
    /* step 3: change `input_txt' to this sequence, so gnu_parallelize
    will read our 1:count .txt file line by line, and save our old
    input file to pass to the program call */
    local manual_inputs `input_txt'
    local input_txt $tmp/index_sequence_`randnum'.txt
    
    /* step 4: tell our temporary do file to read the manual_override
    program call using a specific index line */
    file write group_dofile "file open manual_lines using `manual_inputs', read" _n
    file write group_dofile "local index_counter = 1" _n
    file write group_dofile "file read manual_lines line" _n
    file write group_dofile "while r(eof)==0 {" _n
    file write group_dofile "if `index_counter"
    file write group_dofile "' == 1"
    file write group_dofile " {" _n
    file write group_dofile "local program_command `line"
    file write group_dofile "'" _n
    file write group_dofile "}" _n
    file write group_dofile "file read manual_lines line" _n
    file write group_dofile "local index_counter = `index_counter"
    file write group_dofile "' + 1" _n
    file write group_dofile "}" _n
    file write group_dofile "file close manual_lines" _n

    /* step 5: execute this manual program call  */
    file write group_dofile "`program_command"
    file write group_dofile "'" _n
    file close group_dofile
    
    /* step 6: put the command to remove this temporary index text file into a local */
    local remove_manual_index "rm `input_txt'"
  }

  /* if we don't have manual override, we need to assemble the program
  call using shell variables from our input .txt file */
  if mi("`manual_input'") {
    
    /* having a first pre-comma (varlist or otherwise) shifts all the
    variables coming in from cat - so need two loops here */
    file write group_dofile "`program' "
    if !mi("`pre_comma'") {

      /* all following options will be passed from the shell in
      sequence, as they are read from the text file. if there is a
      pre-comma argument, that will take the position `1' and the other
      options will start at `2' */
      local option_index 2

      /* write out any arguments before the options, which will be
      couched in the bash var `1' */
      file write group_dofile "\`1"
      file write group_dofile "'"
    }

    /* if no initial vars, start at 1 */
    else {
      local option_index 1
    }

    /* if there are options, add the comma. */
    if !mi("`options'") {
      file write group_dofile ","
    }

    /* now deal with the options */
    foreach option in `options' {

      /* write the option name */
      file write group_dofile " `option'("

      /* write the option variable index number */
      file write group_dofile "\``option_index'"
      file write group_dofile"'"
      file write group_dofile ")"

      /* bump up the index for the next loop through */
      local option_index = `option_index' + 1
    }

    /* if there are additional static options across all lines, add those here */
    file write group_dofile " `static_options'"
    
    /* now finish the program call line and close the script. */
    file write group_dofile _n
    file close group_dofile
  }
  
  /* save working directory, then change to scratch */
  local workdir `c(pwd)'
  cd $tmp

  /* use the script we just wrote - in parallel! */
  !cat `input_txt' | parallel --gnu --progress --eta --delay 2.5 -j `max_jobs' "stata -e do parallelizing_dofile_`randnum' {}"

  /* remove our text file, if specified */
  if !mi("`rmtxt'") {
    rm `input_txt'
  }

  /* remove log and dofile, if specified */
  if mi("`diagnostics'") {
    rm parallelizing_dofile_`randnum'.do
  }

  /* remove the manual override's temporary index file - if not
  needed, this is just an empty local */
  //`remove_manual_index'
  
  /* change back to working directory */
  cd `workdir'
}
end
/* *********** END program gnu_parallelize ***************************************** */


/**********************************************************************************/
/* program extract_collapse_prog - assists gnu_parallelize                        */
/**********************************************************************************/
cap prog drop extract_collapse_prog
prog def extract_collapse_prog
{

  /* only need the program name (anything) and the location (string) */
  syntax anything, progloc(string) randnum(string)

  qui {

    /* step 1 - get the line number in the do file that corresponds with
    the start of the program. save to a temp file - not sure if there is
    another way to get stdout into a stata macro */
    !grep -n "cap prog drop `anything'" `progloc' | sed 's/^\([0-9]\+\):.*$/\1/'  | tee $tmp/linenums_`randnum'.txt

    /* step 2 - same for the end of the program. add a new line to the file */
    !grep -n "END program `anything'" `progloc' | sed 's/^\([0-9]\+\):.*$/\1/' >> $tmp/linenums_`randnum'.txt

    /* get the line nums into macros */
    file open lines_file using $tmp/linenums_`randnum'.txt, read
    file read lines_file line
    local first_line `line'
    file read lines_file line
    local last_line `line'
    local last_line_plus_1 = `last_line' + 1
    file close lines_file

    /* step 4 - extract the section of the do file between those line
    nums and save to $tmp/tmp_prog_extracted.do */
    !sed -n '`first_line',`last_line'p;`last_line_plus_1'q' `progloc' > $tmp/tmp_prog_extracted_`randnum'.do

    /* remove the temp file */
    !rm $tmp/linenums_`randnum'.txt
  }
}
end
/* *********** END program extract_collapse_prog ***************************************** */


/**********************************************************************************/
/* program prep_gnu_parallel_input_file - assists gnu_parallelize                 */
/**********************************************************************************/
cap prog drop prep_gnu_parallel_input_file
prog def prep_gnu_parallel_input_file
{

  /* we just need the output file name, and the list to be split into separate lines */
  syntax anything, in(string)

  /* open the output file for writing to */
  file open output_file using `anything', write replace

  /* tokenize the input var */
  tokenize `in'

  /* loop over all the individual inputs and write to a new line */
  while "`*'" != "" {
    file write output_file "`1'" _n
    macro shift
  }

  /* close the file handle */
  file close output_file

  /* print an output message */
  disp _n "input file for gnu_parallelize written to `anything'"
}
end
/* *********** END program prep_gnu_parallel_input_file ***************************************** */
