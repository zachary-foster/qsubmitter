#' Submit jobs to qsub over ssh
#'
#' This function submits commands to qsub (SGE) over ssh.
#' It assumes password-less ssh keys are set up.
#'
#' @param command (\code{character}) One or more commands to run
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param parallel (\code{logical} of length 1) If \code{TRUE}, run jobs in parallel.
#' Default: \code{FALSE}
#' @param remote_cwd (\code{character} of length 1)
#' What to set the current working directory to on the remote server before running jobs.
#' Default: The home directory on the remote server.
#' @param runtime_folder (\code{character} of length 1)
#' The location to save the runtime error, output, and qsub submission script.
#' Default: A unique name with timestamp, random string, and first command in job.
#' @param wait (\code{logical} of length 1) If \code{TRUE}, the function will wait for all
#' jobs to complete before exiting.
#' Default: TRUE
#' @param cores (\code{numeric}) The number of cores to use.
#' Default: 1
#' @param print_track (\code{logical} of length 1) If \code{TRUE}, the progress of the jobs is periodically printed.
#' Default: TRUE
#' @param print_info (\code{logical} of length 1) If \code{TRUE}, information on the job is printed
#' Default: TRUE
#' @param echo (\code{logical} of length 1) If \code{TRUE}, the command to be executed is printed
#' Default: TRUE
#' @param stdout (\code{logical} or \code{character} of length 1) If \code{TRUE}, print or save the standard output of the job.
#' @param stderr (\code{logical} or \code{character} of length 1) If \code{TRUE}, print or save the standard error of the job.
#' @param max_print (\code{numeric}) The maximum number of characters to print each time something is printed.
#' @export
#'
#' @examples
#' \dontrun{
#'
#' remote <- remote_server$new(server = "shell.somewhere.edu", user = "joeshmo", port = 345)
#' qsub(c('echo "test 1" > test1.txt', 'echo "test 2" > test2.txt'), remote, runtime_folder = "~/test/out")
#' }
qsub <- function(command, remote, parallel = FALSE, remote_cwd = NULL,
                 runtime_folder = "qsub_records", wait = TRUE, cores = 1,
                 print_track = FALSE, print_info = TRUE, echo = TRUE,
                 stdout = FALSE, stderr = FALSE, max_print = 10000) {

  my_print <- function(content) {
    content <- paste0(content, collapse = "\n")
    if (nchar(content) > max_print) {
      content <- paste0(strtrim(content, max_print), " ...")
    }
    message(paste0(content, "\n"))
  }

  first_call <- basename(strsplit(command[1], " ")[[1]][1])
  if (is.null(remote_cwd)) {
    remote_cwd <- ssh_command("echo $HOME", remote, quiet = TRUE)
  }
  if (grepl("^/", runtime_folder)) { # is an absolute path
    runtime_path <- runtime_folder
  } else {
    runtime_path <- file.path(remote_cwd, runtime_folder)
  }
  # Echo command ----------------------------------------------------------------------------------
  if (echo) {
    my_print(paste("Command:\n", paste0(command, collapse = "\n")))
  }

  # Make remote output directory ------------------------------------------------------------------
  ssh_command(paste("mkdir -p", runtime_path), remote, quiet = TRUE)

  # Make submit script ----------------------------------------------------------------------------
  script_text <- make_submit_script(command,  parallel = parallel,
                                    out_file = runtime_path, err_file = runtime_path,
                                    name = first_call, cores = cores)
  script_name <-  paste0("qsub_", random_char(7), "_", first_call, ".sh")
  local_path <- file.path(tempdir(), script_name)
  remote_path <- file.path(runtime_path, script_name)
  cat(script_text, file = local_path)
  rsync_push(local_path, remote_path, remote = remote)


  # Sumbit job ------------------------------------------------------------------------------------
  qsub_call <- paste("cd", remote_cwd, ";", "qsub", remote_path)
  ssh_result <- ssh_command(qsub_call, remote, quiet = TRUE)
  job_id <- stringr::str_match(ssh_result[[1]][2], "Your job(-array)? ([0-9]+)[ .]")[1,3]

  # Rename submission script ----------------------------------------------------------------------
  new_script_name <- paste0(first_call, ".i", job_id, ".sh")
  new_remote_path <-  file.path(runtime_path, new_script_name)
  ssh_command(paste("mv", remote_path, new_remote_path), remote, quiet = TRUE)

  # Print info ------------------------------------------------------------------------------------
  if (print_info) {
    my_print(paste0("Job ", job_id," sumbitted."))
    my_print(paste("Submission script:", new_remote_path))
    my_print(paste("Current working directory:", remote_cwd))
    start_time <- Sys.time()
    my_print(paste0("Started: ", start_time))
  }

  # Wait for job(s) to complete -------------------------------------------------------------------
  if (wait) {
    wait_for_qsub(remote, job_id, quiet = ! print_track)
    if (print_info) {
      end_time <- Sys.time()
      run_time <- end_time - start_time
      my_print(paste("Finished:", end_time))
      my_print(paste("Duration:", round(run_time), units(run_time)))
    }
  }

  # Get standard error and output -----------------------------------------------------------------


  remote_stdout_path <- file.path(runtime_path, paste0(first_call, ".o", job_id))
  if (parallel) {
    paste0("ls -1 ", runtime_path, # list file names
           " | grep '\\.o", job_id, "'", # find files for this job
           " | sed 's|^.*$|", runtime_path, "/&|'", # Add directory to names
           " | xargs tail -n +1", # combine the output of each file
           " > ", remote_stdout_path) %>%
      ssh_command(remote = cgrb, quiet = TRUE)  %>%
      `[[`(1)
  }
  if (stdout != FALSE) {
    if (is.character(stdout) && length(stdout) == 1) {
      local_stdout_path <- stdout
      rsync_pull(local_stdout_path, remote_stdout_path, remote, quiet = TRUE)
      my_print(paste("Standard output:", local_stdout_path))
    } else {
      local_stdout_path <- tempfile()
      rsync_pull(local_stdout_path, remote_stdout_path, remote, quiet = TRUE)
      my_print(paste("Standard output:\n\n", readChar(local_stdout_path, nchars = 100000)))
    }
  } else if (print_info) {
    my_print(paste("Standard output:", remote_stdout_path))
  }
  remote_stderr_path <- file.path(runtime_path, paste0(first_call, ".e", job_id))
  if (parallel) {
    paste0("ls -1 ", runtime_path, # list file names
           " | grep '\\.e", job_id, "'", # find files for this job
           " | sed 's|^.*$|", runtime_path, "/&|'", # Add directory to names
           " | xargs tail -n +1", # combine the output of each file
           " > ", remote_stderr_path) %>%
      ssh_command(remote = cgrb, quiet = TRUE)  %>%
      `[[`(1)
  }
  if (stderr != FALSE) {
    if (is.character(stderr) && length(stderr) == 1) {
      local_stderr_path <- stderr
      rsync_pull(local_stderr_path, remote_stderr_path, remote, quiet = TRUE)
      my_print(paste("Standard error:", local_stderr_path))
    } else {
      local_stderr_path <- tempfile()
      rsync_pull(local_stderr_path, remote_stderr_path, remote, quiet = TRUE)
      my_print(paste("Standard error:\n\n", readChar(local_stderr_path, nchars = 100000)))
    }
  } else if (print_info) {
    my_print(paste("Standard error:", remote_stderr_path))
  }


  # Return job id ---------------------------------------------------------------------------------
  return(invisible(job_id))
}


#' Wait for qsub job
#'
#' Wait for qsub job to complete or fail.
#'
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param job_id (\code{character} of length 1) The id of the job.
#' @param quiet (\code{logical} of length 1) Supress messeges.
#' Default: TRUE
#'
#' @return NULL
wait_for_qsub <- function(remote, job_id, quiet = TRUE) {
  wait_time <- 3 # initial wait time
  wait_increase <- 1.3 # Amount to increase wait time by each check
  max_wait <- 60*3 # do not increase wait time after this point

  for (unused_index in 1:10000) {
    Sys.sleep(wait_time)
    status <- qstat(remote, quiet = TRUE)
    status <- status[status$job_id == job_id, ] # only consider jobs for this submission
    state_key <- c(deleted = "d", error = "E", running = "r", restarted = "R", suspended = "sS",
                   transfering = "t", threshold = "T", waiting = "w")
    if (nrow(status) > 0) {
      state_count <- vapply(state_key, FUN.VALUE = numeric(1),
                            function(x) sum(grepl(status$state, pattern = paste0("[", x, "]+"))))

    } else {
      state_count <- setNames(rep(0, length(state_key)), names(state_key))
    }
    if (! quiet) {
      displayed_count <- state_count[state_count > 0]
      count_text <- paste(names(displayed_count), displayed_count, sep = ": ", collapse = ", ")
      if (length(displayed_count) == 0) {
        count_text <- "All jobs complete"
      }
      current_time <- Sys.time()
      message(paste0(current_time, " - ", count_text))
    }

    if (state_count[c("error")] > 0) {
      stop(paste0("Job '", job_id, "' failed to execute."))
    }

    if (state_count[c("deleted")] > 0) {
      warning(paste0("Job '", job_id, "' was deleted."))
    }

    if (sum(state_count[c("running", "waiting", "transfering")]) == 0) {
      break()
    }

    if (wait_time < max_wait) { wait_time <- wait_time * wait_increase }
  }
}


#' Make qsub submission script
#'
#' Makes the text for a qsub submission script.
#'
#' @param command (\code{character}) One or more commands to run
#' @param parallel (\code{logical} of length 1) If \code{TRUE}, run jobs in parallel.
#' @param out_file (\code{character} of length 1) Where job runtime output is stored.
#' @param err_file (\code{character} of length 1) Where job error output is stored.
#' @param name (\code{character} of length 1) The job name.
#' @param cores (\code{numberic}) The number of cores to use.
#' Default: 1
#'
#' @return  (\code{character} of length 1) The text for a qsub submission script.
make_submit_script <- function(command, parallel = FALSE, out_file = NULL, err_file = NULL, name = NULL,
                               cores = 1) {
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

  command <- comment_command(command)

  if (parallel) {
    execute <- "eval ${COMMANDS[$i]}"
  } else {
    execute <- paste(sep = "\n",
                     'for cmd in "${COMMANDS[@]}"',
                     "do",
                     "    eval $cmd",
                     "done\n")
  }
  file_text <- paste(sep = '\n',
                     "#!/bin/bash",

                     "#$ -cwd",
                     "#$ -S /bin/bash",
                     paste0("#$ -N ", name),
                     paste0("#$ -o ", out_file),
                     paste0("#$ -e ", err_file),
                     paste0("#$ -pe thread ", cores),
                     "#$ -V",
                     ifelse(parallel, paste0("#$ -t 1-", length(command), ":1"), ""),
                     ifelse(parallel, 'i=$(expr $SGE_TASK_ID - 1)', ""),
                     'echo -n "Running on: "',
                     "hostname",
                     'echo "SGE job id: $JOB_ID"',
                     "date",
                     paste0('COMMANDS=(', paste(command, collapse = ' '), ')'),
                     execute)
  return(file_text)
}


#===================================================================================================
#' Run a commmand on a remote server
#'
#' Run a commmand on a remote server using ssh.
#'
#' @param command (\code{character}) The command to run
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param quiet (\code{logical} of length 1) Supress messeges.
#' @param stderr Passed to the \code{stderr} argument of \code{\link{system2}}.
#' If \code{TRUE}, Mix in standard error with output.
#' @param prompt (\code{character} of length 1) Character to display in front of commands.
#' Defualt: timestamp.
#' @param ... Passed to \code{\link{system2}}.
#'
#' @return  (\code{character}) The standard output of the command.
#'
#' @export
#'
#' @examples
#' \dontrun{
#'
#' remote <- remote_server$new(server = "shell.somewhere.edu", user = "joeshmo", port = 345)
#' ssh_command(c('"echo test1", "echo test2"), remote)
#' }
ssh_command <- function(command, remote, quiet = FALSE, stderr = FALSE, prompt = NULL, ...) {
  do_one <- function(command) {
    if (is.null(prompt)) {
      time <- strsplit(as.character(Sys.time()), split = " ")[[1]][2]
      prompt = paste0("[", time, "]$ ")
    }
    if (!quiet) { message(paste0(prompt, command, "\n")) }
    command <- comment_command(command)
    ssh_args <- c(paste0(remote$user, "@", remote$server), "-p", remote$port, "-t -t", command)
    result <- system2("ssh", ssh_args, stdout = TRUE, stderr = FALSE, ...)
    result <- gsub("^\\s+|\\s+$", "", result) # remove whitespace
    if (!quiet) {
      message(paste0(result, "\n"))
    }
    return(result)
  }
  results <- lapply(command, do_one)
  names(results) <- command
  invisible(results)
}



#===================================================================================================
#' Prepare a command to be an argument
#'
#' Prepare a command to be the argument of another command.
#'
#' @param command (\code{character}) The command to convert.
#' @param quote (\code{character} of length 1) What quote to use.
#' Default: Choose quote type based on quote types in command.
#'
#' @return (\code{character})
comment_command <- function(command, quote = NULL) {
  do_one <- function(command, quote) {
    if (is.null(quote)) {
      if (grepl("'", command) && ! grepl('"', command)) { # command has only single quotes
        quote <- '"'
      } else if (! grepl("'", command) && grepl('"', command)) { # command has only double quotes
        quote <- "'"
      } else if (! grepl("'", command) && ! grepl('"', command)) { # does not have either quote type
        quote <- "'"
      } else { # has both quote types
        quote <- "'"
        command <- gsub(command, pattern = paste0("[^\\]", quote), replacement = paste0("\\", quote), fixed = TRUE)
      }
    }
    command <- gsub(command, pattern = '[^\\]\n', replacement = '\\n', fixed = TRUE)
    paste0(quote, command, quote)
  }

  vapply(command, do_one, character(1), quote = quote)
}



#===================================================================================================
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






#===================================================================================================
#' Random character
#'
#' Make a random character of a specified length
#'
#' @param count (\code{numeric} of length 1) The length of the random string.
random_char <- function(count = 5) {
  possible <- c(letters, 0:9)
  paste0(possible[sample(seq_along(possible), count)], collapse = "")
}




#' Return parsed qstat table
#'
#' Execute \code{qstat} and parse the qstat table output
#'
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param quiet (\code{logical} of length 1) Supress messeges.
#' Default: \code{FALSE}
#'
#' @export
#'
#' @return \code{data.frame}
#'
#' @examples
#' \dontrun{
#'
#' remote <- remote_server$new(server = "shell.somewhere.edu", user = "joeshmo", port = 345)
#' qstat(remote)
#' }
qstat <- function(remote, quiet = FALSE) {
  prepare_row <- function(row) {
    split_data <- strsplit(row, split = " +", perl = TRUE)[[1]]
    split_data <- c(split_data[1:5], paste0(split_data[6:7], collapse = "-"), split_data[8:length(split_data)])
    if (length(split_data) == 7) { # no queue assinged
      split_data <- c(split_data[1:6], NA, split_data[7])
    }
    if (length(split_data) == 8) { # no task id defined
      split_data <- c(split_data[1:8], NA)
    }
    return(split_data)
  }

  qstat_output <- ssh_command("qstat", remote, quiet = quiet)[[1]]
  if (length(qstat_output) == 0) {
    data <- data.frame(matrix(ncol = 9, nrow = 0))
  } else {
    header <- qstat_output[1]
    content <- qstat_output[3:length(qstat_output)]
    split_data <- lapply(content, prepare_row)
    data <- data.frame(do.call(rbind, split_data))
  }
  colnames(data) <- c("job_id", "prior", "name", "user", "state", "submit_time",
                      "queue", "slots", "task_id")
  invisible(data)
}



#' Run R commands on remote server
#'
#' Run R commands on remote server.
#' Variables in the local envrioment can be transfered to the remote computer.
#'
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param variables (\code{list}) Variables that the \code{expression} relies on.
#' Their content will be deparsed and transfered to the remote server.
#' @param expression An expression to run on a remote server.
#' @param ... Passed to \code{\link{ssh_command}}
#'
#' @export
#'
#' @examples
#' \dontrun{
#' remote <- remote_server$new(server = "shell.somewhere.edu", user = "joeshmo", port = 345)
#' a = 1:10
#' f = function(x) {
#'   x = x + 2
#'   x*2
#' }
#' remote_r(remote, list(a, f), {
#'   a = a + 3
#'   f(a)
#' })
#' }
remote_r <- function(remote, variables = list(), expression, ...) {
  # Make lines to define variables
  if (length(variables) > 0) {
    var_substitution <- substitute(variables)
    temp_path <- tempfile()
    on.exit(file.remove(temp_path))
    lapply(seq_along(variables) + 1,
           function(x) {
             name <- deparse(var_substitution[[x]])
             dump(name, file = temp_path, append = TRUE)
           })
    define_variables <- readLines(temp_path)
  } else {
    define_variables <- c()
  }

  # Make lines for the expression
  parsed_expression <- deparse(substitute(expression))

  exp_substitution <- substitute(expression)
  parsed_expression <- unlist(lapply(2:length(exp_substitution),
                                     function(x) deparse(exp_substitution[[x]])))

  # Make lines for quitting R
  quit_r <- c("quit()")

  # Execute commands
  input_lines <- c(define_variables, parsed_expression, quit_r)
  invisible(ssh_command("R --vanilla --quiet", remote, input = input_lines, ...))
}


