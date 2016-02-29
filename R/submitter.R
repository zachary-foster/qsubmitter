#' Submit jobs to qsub over ssh
#'
#' This function submits commands to qsub (SGE) over ssh.
#' It assumes password-less ssh keys are set up.
#'
#' @param command (\code{character}) One or more commands to run
#' @param remote (\code{\link{remote}})
#' @param parallel (\code{logical} of length 1)
#'
#' @export
qsubmit <- function(command, remote, parallel = FALSE, runtime_folder = NULL) {
  first_call <- strsplit(command[1], " ")[[1]][1]
  if (is.null(runtime_folder)) {
    runtime_folder <- file.path("~", paste0(first_call, "_qsubmitter_runtime"))
  }

  ssh_command(paste("mkdir", runtime_folder), remote)

  script_text <- make_submit_script(command,  parallel = parallel,
                                    out_file = runtime_folder, err_file = runtime_folder)
  script_path <- file.path(runtime_folder, paste0(first_call, "_submit.sh"))
  save_script_call <- paste0("echo '", script_text, "' > ", script_path)
  ssh_command(save_script_call, remote, quote = "'")

  qsub_call <- paste("qsub", script_path)
  ssh_command(qsub_call, remote)
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

  # command <- comment_command(command)

  if (parallel) {
    execute <- "eval ${COMMANDS[$i]}"
  } else {
    execute <- paste(sep = "\n",
                     "for index in ${COMMANDS[@]};",
                     "do",
                     "eval ${COMMANDS[$index]}",
                     "done")
  }
  file_text <- paste(sep = '\n',
                     "#!/bin/bash",

                     "#$ -cwd",
                     "#$ -S /bin/bash",
                     paste0("#$ -N ", name),
                     paste0("#$ -o ", out_file),
                     paste0("#$ -e ", err_file),
                     "#$ -V",
                     ifelse(parallel, paste0("#$ -t 1-", length(command), ":1"), ""),
                     ifelse(parallel, 'i=$(expr $SGE_TASK_ID - 1)', ""),
                     'echo -n "Running on: "',
                     "hostname",
                     'echo "SGE job id: $JOB_ID"',
                     "date",
                     paste0('COMMANDS=("', paste(command, collapse = '" "'), '")'),
                     execute)
  # file_text <- gsub(file_text, pattern = "'", replacement = "\\'", fixed = TRUE)
  # # file_text <- gsub(file_text, pattern = " ", replacement = "\\ ", fixed = TRUE)
  # file_text <- gsub(file_text, pattern = '"', replacement = '\\"', fixed = TRUE)
  return(file_text)
}


#' Run a commmand on a remote server
#'
#' Run a commmand on a remote server using ssh.
#'
#' @param command (\code{character}) The command to run
#' @param remote (\code{\link{remote}})
#'
#' @export
ssh_command <- function(command, remote, quote = "'") {
  command <- comment_command(command, quote)
  ssh_call <- c("ssh", paste0(remote$user, "@", remote$server), "-p", remote$port, "-t", command)
  ssh_call <- paste(ssh_call, collapse = " ")
  system(ssh_call, ignore.stderr = FALSE)
  cat(command)
}



#' Prepare a command to be an argument
#'
#' Prepare a command to be an argument
#'
#' @param command (\code{character}) The command to convert.
#'
#' @return (\code{character})
comment_command <- function(command, quote = "'") {
  command <- gsub(command, pattern = paste0("[^\\]", quote), replacement = paste0("\\", quote), fixed = TRUE)
  command <- gsub(command, pattern = '[^\\]\n', replacement = '\\n', fixed = TRUE)
  command <- paste0(quote, command, quote)
  return(command)
}



#' Class for remote server
#'
#' @description Stores information needed to connect to a remote user
#' @docType class
#' @export
#' @return Object of \code{\link{R6Class}} with methods for communication with lightning-viz server.
#' @format \code{\link{R6Class}} object.
#' @field server Address of the server
#' @field user User name
#' @field port Port of remote server
#' @section Methods:
#' \describe{
#'   \item{new}{Make new instance of this class}
#'   }
remote_server <- R6::R6Class("remote",
                             public = list(
                               server = NA,
                               user = NA,
                               port = 22,
                               initialize = function(server, user, port = 22) {
                                 if (!missing(server)) self$server <- server
                                 if (!missing(user)) self$user <- user
                                 if (!missing(port)) self$port <- port
                               }
                             )
)
