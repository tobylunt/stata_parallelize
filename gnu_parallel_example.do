/*
INSTALLING GNU PARALLEL:
N.B.: this code leverages GNU parallel for speeding up data generation. this needs to be installed on your local machine. to do so, use the following steps in your shell:

wget https://git.savannah.gnu.org/cgit/parallel.git/plain/src/parallel
chmod 755 parallel
cp parallel sem
mv parallel sem dir-in-your-$PATH/bin/

adding to your path will vary, but following this format should work:
/afs/northstar/users/l/lunt/bin
*/

/*
WHAT THIS TOY EXAMPLE DOES:

Creates pc11 population means at the state level, with the following steps:

 write state names to a text file.
1. open pc11 and keep the current state
2. calculate the mean population and write it somewhere
*/


/*************************************/
/* (1) define parallelizable program */
/*************************************/

cap prog drop gnu_parallel_example
prog def gnu_parallel_example
{

  /* the only thing we need is a state name, plus an unused option as an example */
  syntax anything [, useless_option(string)]

  /* read in the population census */
  use ~/iec1/pc11/pc11_pca_state_clean.dta, clear

  /* keep for the specified state */
  keep if pc11_state_id == "`anything'"

  /* calculate mean population */
  qui sum pc11_pca_tot_p

  /* output example 1: write to existing csv */
  file open output_csv_appended using "$tmp/parallel_example_output.csv", write append
  file write output_csv_appended "`anything', `r(mean)'" _n
  file close output_csv_appended
  
  /* output example 2: save to individual file */
  file open output_csv_individual using "$tmp/parallel_example_output_`anything'.csv", write replace
  file write output_csv_individual "state, avg_pop" _n
  file write output_csv_individual "`anything', `r(mean)'" _n
  file close output_csv_individual
}
end
/* *********** END program gnu_parallel_example ******************************* */

/* NOTE: the above "end program ..." line is CRUCIAL if you have your
parallelizing code in your same do-file. this is how the gnu_parallel
program extracts your program and reads it in batch mode. */


/**********************************************************/
/* (2) write input arguments (state names) to a text file */
/**********************************************************/

/* NOTE: if you only have one option to pass to your code, such as the
state, then you do NOT need to file-write a separate input txt
file. instead, just call "prep_input_file($statelist)" or whatever
your input happens to be. */

/* initialize a text file that will have our state names saved in it, one per line. */
file open arguments_file using "$tmp/parallel_args.txt", write replace

/* set the list of states */
global statelist 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35

/* loop over the states, and write each name to a new line */
foreach state in $statelist {

  /* write the name and a newline character */
  file write arguments_file "`state' useless_option_input" _n

  /* NB: if you need to write quotes, this must be done carefully due
  to limitations of file write. e.g.: file write groupnumfile
  `"`=char(34)'"' _n */
}

/* close the group numbers text file */
file close arguments_file

/* if you want, you can have your program write out your calcs to an
existing file, to show this, we initiate a csv here that will be added
to. */
file open output_csv using "$tmp/parallel_example_output.csv", write replace
file write output_csv "state, avg_pop" _n
file close output_csv


/********************************/
/* (3) call program in parallel */
/********************************/

/* CASE 1: looping over states, using program in this same do file. */
gnu_parallelize, max_jobs(8) input_txt($tmp/parallel_args.txt) program(gnu_parallel_example) progloc(~/iecmerge/ra/lunt/gnu_parallel/gnu_parallel_example.do) extract_prog prep_input_file($statelist) pre_comma diag trace tracedepth(2)

/* CASE 2: more than one option required; program in a different do-file.
 the pre_comma
option means there is input before the options - in this example, that
input is the main `anything' that contains our state name. the
"options(useless_option)" command specifies that we have one option in
our program, which is named useless_option. */
gnu_parallelize, max_jobs(8) input_txt($tmp/parallel_args.txt) program(gnu_parallel_example) progloc(~/iecmerge/ra/lunt/gnu_parallel/gnu_parallel_external_program_example.do) options(useless_option) maxvar diag pre_comma


/***********************/
/* (4) clean up output */
/***********************/

/* the program compiled output itself into
$tmp/parallel_example_output.csv, as well as in separate state-level
files. first check out the compiled files */
insheet using $tmp/parallel_example_output.csv, clear
list
clear

/* now we compile the state files here as an example. append the state
output files together */
insheet using $tmp/parallel_example_output_01.csv, clear
drop _all
save $tmp/gnu_parallel_merged.dta, replace
foreach state in `statelist' {
  insheet using $tmp/parallel_example_output_`state'.csv, clear  
  append using $tmp/gnu_parallel_merged.dta
  save $tmp/gnu_parallel_merged.dta, replace
}
