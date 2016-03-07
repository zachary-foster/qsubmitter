#===================================================================================================
#' Rsync files to remote server.
#'
#' Copys files to a remote server using rsync.
#'
#' @param local_path (\code{character})
#' Paths of files on the local machine for transfer.
#' @param remote_path (\code{character})
#' Paths of files on the remote machine for transfer.
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param quiet (\code{logical} of length 1) Supress messeges.
#' Default: TRUE
#'
#' @export
rsync_push <- function(local_path, remote_path, remote, quiet = TRUE) {
  command <- paste0("rsync -avh -e 'ssh -p ", remote$port, "' ",
                    paste(local_path, collapse = " "), " ", remote$user, "@", remote$server, ":", remote_path)
  invisible(system(command, ignore.stdout = quiet, ignore.stderr = quiet))
}


#===================================================================================================
#' Rsync files from remote server.
#'
#' Copys files from a remote server using rsync.
#'
#' @param local_path (\code{character})
#' Paths of files on the local machine for transfer.
#' @param remote_path (\code{character})
#' Paths of files on the remote machine for transfer.
#' @param remote (\code{\link{remote_server}}) The remote server information.
#' @param quiet (\code{logical} of length 1) Supress messeges.
#' Default: TRUE
#'
#' @export
rsync_pull <- function(local_path, remote_path, remote, quiet = TRUE) {
  command <- paste0("rsync -avh -e 'ssh -p ", remote$port, "' ",
                    remote$user, "@", remote$server, ":", remote_path, " ",
                    local_path)
  system(command, ignore.stdout = quiet, ignore.stderr = quiet)
}
