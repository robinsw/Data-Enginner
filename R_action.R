
#' Run a CAS Action by Name
#'
#
runAction <-  function(CASorCASTab='', actn, check_errors=FALSE, ...) {
   if ( is.null(CASorCASTab) || ( is.character(CASorCASTab) && nchar(CASorCASTab) == 0 ) )
      {
      args <- list(...)
      if ( !is.null(args[['table']]) && class(args[['table']]) == 'CASTable' )
         {
         CASorCASTab <- args[['table']]@conn
         }
      }
   if (class(CASorCASTab) == 'CASTable')
      {
      tp  = swat::gen.table.parm(CASorCASTab)
      cas = CASorCASTab@conn
      pms = list('caz'=cas, 'actn'=actn, 'table'=tp, ...)
      res <- do.call('casRetrieve', pms)
      }
   else
      {
      cas = CASorCASTab
       if ( is.null(CASorCASTab) || ( is.character(CASorCASTab) && nchar(CASorCASTab) == 0 ) )
       {
           stop('No CAS connection was specified for the action')
       }
      if (actn == 'builtins.loadActionSet')
         {
         stopifnot(class(cas) == 'CAS') 
         actionSet=list(...)[[1]]
         res <- casRetrieve(cas, 'builtins.loadActionSet', actionSet=actionSet)
         gen.functions(cas, actionSet)
         swat::check_for_cas_errors(res)
         }
      else if (actn == 'builtins.defineActionSet')
         {
         stopifnot(class(cas) == 'CAS') 
         actionSet=list(...)$name
         res <- casRetrieve(cas, 'builtins.defineActionSet', ...)
         gen.functions(cas, actionSet)
         swat::check_for_cas_errors(res)
         }
      else
         {
         pms = list('caz'=cas, 'actn'=actn, ...)
         res <- do.call('casRetrieve', pms)
         }
      }

  if (check_errors)
     check_for_cas_errors(res)

  as.list(res$results)
}

runSasCode <- function(cas, code=""){
  res <- casRetrieve(cas, 'dataStep.runCode', code=code)
  swat::check_for_cas_errors(res)
  return (res$results)
}

#' Load a CAS Action Set
#'

loadActionSet <- function(conn, actionSet=""){
  stopifnot(class(conn) == 'CAS')
  res <- casRetrieve(conn, 'builtins.loadActionSet', actionSet=actionSet)
  gen.functions(conn, actionSet)
  swat::check_for_cas_errors(res)
}


#' List CAS Action Sets
#'

listActionSets <- function(conn){
  stopifnot(class(conn) == 'CAS')
  res <- casRetrieve(conn, 'builtins.actionSetInfo', all='FALSE')
  swat::check_for_cas_errors(res)
  as.list(res$results$setinfo)
}

#' Utility function that is run from several exported functions.
#' 
 
casRetrieve <-  function(caz, ...) {
  if (class(caz)=='CAS'){
    return(caz$retrieve(..., '_messagelevel'=as.character(getOption('cas.message.level'))))
  }
  if (class(caz) =='CASTable'){
    return(caz$retrieve(caz@conn, ..., '_messagelevel'=as.character(getOption('cas.message.level'))))
  }
}

gen.functions2 <-  function(cas, actionSet) {
  #message(paste("get action set list", Sys.time()))
  args <- list(quote(cas), 'builtins.reflect', actionSet=actionSet, showLabels=FALSE)
  if ( 'reflection.levels' %in% cas$serverFeatures ) { args$levels <- 1 }
  res = do.call(swat::runAction, args)
  #message(paste("got action set list", Sys.time()))
  env = globalenv()
  if (length(res[[1]]) > 0)
     for(i in 1:length(res[[1]]$actions))
        {
        #message(paste("for action", i, Sys.time()))
        name = res[[1]]$actions[[i]]$name
        if (as.logical(getOption('cas.gen.function.sig')))
           {
           str  = ''
           str2 = ''
           for (parms in res[[1]]$actions[[i]]$params)
              {
              #message(paste("for parm", Sys.time()))
              str  = paste(str,  ", `", parms$name, '`=NULL',  sep = '')
              str2 = paste(str2, ", `", parms$name, '`=`', parms$name, "`", sep = '')
              }
           fname = paste("cas.", actionSet, ".", name, sep="")
           str1  = paste(fname,  " <- function(CASorCASTab", str, sep = '')
           val   = paste(str1, ", ...) {swat::runAction(CASorCASTab=NULL, '", actionSet, ".", name, "'", str2, ", ...) } ", sep="")
           defn = eval(parse(text=val, env))
           environment(defn) <- env
           setGeneric(name=fname, def=defn, package='swat', where=env)
           #message(paste("function defined", Sys.time()))
           }
        else
           {
           fname = paste("cas.", actionSet, ".", name, sep="")
           val   = paste(fname, " <- function(CASorCASTab, ...) {swat::runAction(CASorCASTab=NULL, '", paste(actionSet, name, sep="."),  "', ...) } ", sep="")
           defn  = eval(parse(text=val, env))
           environment(defn) <- env
           setGeneric(name=fname, def=defn, package='swat', where=env)
           #message(paste("function defined", Sys.time()))
           }
        }
}

.gen.sig <-  function(cas, actn) {
  #message(paste("get action list", Sys.time()))
  args <- list(quote(cas), 'builtins.reflect', check_errors=TRUE, action=actn, showLabels=FALSE)
  if ( 'reflection.levels' %in% cas$serverFeatures ) { args$levels <- 1 }
  res = do.call(swat::runAction, args)
  #message(paste("got action list", Sys.time()))
  #swat::check_for_cas_errors(res)
  str  = ''
  str2 = ''
  str3 = '   args <- list(...)\n'
  for (parms in res[[1]]$actions[[1]]$params)
     {
     #message(paste("for parm", Sys.time()))

     if (! is.null(parms$isRequired))
        {
        str  = paste(str,  ", `", parms$name, '`',  sep = '')
        str2 = paste(str2, ", `", parms$name, '`=`', parms$name, "`", sep = '')
        }
     else
        if (! is.null(parms$default))
           {
           if (parms$parmType == "string")
              {
              s1   = if (parms$default =='') ' ' else parms$default
              s1   = if (s1 =='\\') "\\\\" else parms$default
              str  = paste(str,  ", `", parms$name, '`="', s1, '"', sep = '')
              }
           else
              str  = paste(str,  ", `", parms$name, '`=', parms$default,  sep = '')

           str2 = paste(str2, ", `", parms$name, '`=`', parms$name, "`", sep = '')
           }
        else
           {
           str  = paste(str,  ", `", parms$name, '`=NULL', sep = '')
           }
     str3 = paste(str3, "   if( ! missing(`",parms$name,"`)) args = c('", parms$name, "'=substitute(`", parms$name, "`), args)\n", sep = '')
     }

  str3 = paste(str3, "   args = c('actn'='", actn, "', args)\n", sep = '')
  str3 = paste(str3, "   args = c('CASorCASTab'=substitute(CASorCASTab), args)\n", sep = '')

  str1 = paste("cas.", actn,  " <- function(CASorCASTab", str, ", ...){\n" , sep = '')
  str1 = paste(str1, str3, "do.call('swat::runAction', args) \n}\n", sep="")

  #message(paste('Defined ', actn, ' as:\n', str1, '\n'))
  return (str1)
}

gen.functions <-  function(cas, actionSet) {
  res = casRetrieve(cas, 'actionSetInfo', 'extensions'=actionSet)
  if (toupper(res$results$setinfo$actionset[1]) != toupper(actionSet))
     message(paste("ActionSet ",actionSet, " not found"))
  else
     {
     actionSet = res$results$setinfo$actionset[1] 
     #message(paste("get action set list", Sys.time()))
     res = casRetrieve(cas, 'listActions', 'actionSet'=actionSet)
     #message(paste("got action set list", Sys.time()))
     if (length(res$results)>0)
        {
        acts = as.data.frame(res$results)[,1]
        env = globalenv()
        for (name in acts)
           {
           #message(paste("for action", Sys.time()))
           if (!as.logical(getOption('cas.gen.function.sig')))
              {
              val = paste("cas.", actionSet, ".", name, " <- function(CASorCASTab=NULL, ...) {swat::runAction(CASorCASTab, '", paste(actionSet, name, sep="."),  "', ...) } ", sep="")
              defn = eval(parse(text=val, env))
              environment(defn) <- env
              fname = paste("cas.", actionSet, ".", name, sep="")
              setGeneric(name=fname, def=defn, package='swat', where=env)
              #message(paste("function defined", Sys.time()))
              }
           else
              {
              val = .gen.sig(cas, paste(actionSet, name, sep="."))
              tryCatch(
                 {
                 defn = eval(parse(text=val, env))
                 environment(defn) <- env
                 fname = paste("cas.", actionSet, ".", name, sep="")
                 setGeneric(name=fname, def=defn, package='swat', where=env)
                 #message(paste("function defined", Sys.time()))
                 }
              , error=function(e)
                 {
                 message(paste("Action ", actionSet, ".", name, " Had invalid syntax: \n", val, sep=""))
                 message(paste("Error was: ", e))
                 message('Defining syntax as function(CASorCASTab, ...) instead. Use listActionParms() to see the actual paramters for this function')
                 val = paste("cas.", actionSet, ".", name, " <- function(CASorCASTab=NULL, ...) {swat::runAction(CASorCASTab, '", paste(actionSet, name, sep="."),  "', ...) } ", sep="")
                 defn = eval(parse(text=val, env))
                 environment(defn) <- env
                 fname = paste("cas.", actionSet, ".", name, sep="")
                 setGeneric(name=fname, def=defn, package='swat', where=env)
                 })
              }
           }
        }
     }
}

#' List CAS Action Parameters by Name
#'

listActionParms <- function(conn, actn, display=TRUE){
  if (typeof(actn) == 'closure')
     actn = toString(substitute(actn))
  if (startsWith(actn, 'cas.'))
     actn = substr(actn, 5, nchar(actn))
  args <- list('builtins.reflect', action=actn, showLabels=FALSE)
  if ( 'reflection.levels' %in% conn$serverFeatures ) { args$levels <- 1 }
  res <- do.call(conn$retrieve, args)
  swat::check_for_cas_errors(res)
  str   <- paste("cas.", actn,'(', sep='')
  plist <- list()
  i     <- 1
  sep   <- ''
  for (parms in res$results[[1]]$actions[[1]]$params)
     {
     str = paste(str, sep, parms$name, '=\"', parms$parmType, '\"', sep=''); sep = ', '
     plist[[i]] = list('name'=parms$name, 'parmType'=parms$parmType)
     i = i +1
     }
  str = paste(str, ')\n')
  if (display)
     { 
     cat(str)
     }
  return (plist)
}