#' Submit jobs to qsub over ssh
#'
#' This function submits commands to qsub (SGE) over ssh.
#' It assumes password-less ssh keys are set up.
#'
#' @param command (\code{character}) One or more commands to run
#' @param parallel (\code{logical} of length 1)
qsubmit <- function(command, remote, user, port = 22, parallel = FALSE, runtime_folder = NULL, working_dir = "~") {
  first_call <- strsplit(command[1], " ")[[1]][1]
  if (is.null(runtime_folder)) {
    runtime_folder <- paste0(first_call, "_runtime_output")
  }

  ssh_command(paste("mkdir", runtime_folder), remote, user, port)

  script_text <- make_submit_script(ommand, parallel = parallel, out_file = runtime_folder,
                                    err_file = runtime_folder)
  script_path <- file.path(working_dir, paste0(first_call, "_submit.sh"))
  paste0("printf '", script_text, "' > ", script_path)

  qsub_call <- paste("qsub", script_path)
  ssh_command(qsub_call, remote, user, port)
}



make_submit_script <- function(command, parallel = FALSE, out_file = NULL, err_file = NULL, name = NULL) {
  first_call <- strsplit(command[1], " ")[[1]][1]
  if (is.null(out_file)) {
    out_file <- paste0(first_call, "_out")
  }
  if (is.null(err_file)) {
    err_file <- paste0(first_call, "_err")
  }
  if (is.null(name)) {
    name <- paste0(first_call, "_job")
  }
  command <- gsub(command, pattern = "`", replacement = "`", fixed = TRUE)
  command <- gsub(command, pattern = "`", replacement = "`", fixed = TRUE)

  header <- paste0("#!/bin/bash
#$ -cwd
#$ -S /bin/bash
#$ -N ", name, "
#$ -o ", out_file, "
#$ -e ", err_file, "
#$ -V
", ifelse(parallel, paste0("#$ -t 1-", length(command), ":1\n"), ""))

  prepare <- paste0(ifelse(parallel, 'i=$(expr $SGE_TASK_ID - 1)', ""), '
echo -n "Running on: "
hostname
echo "SGE job id: $JOB_ID"
date
COMMANDS=(', paste0('"', paste(command, collapse = '" "'), '"'), ')
')

if (parallel) {
  execute <-"eval ${COMMANDS[$i]}"
} else {
  execute <- "for index in ${COMMANDS[@]};
do
eval ${COMMANDS[$index]}
done
"
}
  file_text <- paste0(header, prepare, execute)
 return(file_text)
}



ssh_command <- function(command, remote, user, port = 22) {
  ssh_call <- c("ssh", paste0(user, "@", remote), "-p", port, "-t", paste0('"', command, '"'))
  ssh_call <- paste(ssh_call, collapse = " ")
  system(ssh_call, ignore.stderr = TRUE)
  return(ssh_call)
}
