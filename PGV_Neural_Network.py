 import swat                                                      

 #  Specify the host and port information match your site

 s = swat.CAS("dsascontrol", 8777)

 from IPython.core.display import display                          

 result = s.table.upload(
   path="/home/viya/PGV.csv",
   casout={
     "name":"PGV",
     "replace":True}                                               
   )

 PGV = s.CASTable(result.tableName)                              

 # Display Table Info for PGV

 ti = PGV.table.tableInfo().TableInfo                             
 display(HTML('<h3>Table Information</h3>'))
 display(ti.ix[:,'Name':'JavaCharSet'])
 display(ti.ix[:,'ModTime':])

 # Display Table Details for PGV

 td = PGV.table.tableDetails().TableDetails                       
 display(HTML('<h3>Table Details</h3>'))
 display(td.ix[:,'Node':'VardataSize'])
 display(td.ix[:,'Mapped':])

 # Use the sampling_Stratified action to partition the PGV input
 # data by variable SPECIES into training (70%) and validation
 # (30%) tables.

 result = s.sampling.stratified(                                    
      table={
     "name":"PGV",
     "groupby":[{
       "name":"species"}]},

     partind=True,                                                  

   output={                                                        
     "casout":{
       "name":"PGV_partitioned",
       "replace":True},
       "copyVars":"ALL",
       "partindname":"Partn_Val"                                    
       },

   samppct=70,                                                      
   seed=12345)                                                      

 # Use the map data in the newly added Partn_Val column to create
 # separate CAS tables for Neural Net training and validation.

 trnTable={                                                         
   "name":"PGV_partitioned",
   "where":"1=Partn_Val"}

 vldTable={                                                         
   "name":"PGV_partitioned",
   "where":"0=Partn_Val"}

 -- Use the annTrain action to create and train a MLP neural network
 -- for a nominal target SPECIES.

 results = s.neuralNet.annTrain(                                    
   table=trnTable,
   target="species",

   "nominals"=[{                                                    
     "name":"species"}],

   inputs=[{                                                       
     "name":{
       "sepallength",
       "sepalwidth",
       "petallength",
       "petalwidth"}}
     ],
   listNode="ALL",                                                  
   arch="MLP",                                                     
   hiddens={2},                                                     
   combs={"LINEAR"},                                                
   targetAct="SOFTMAX",                                             
   errorFunc="ENTROPY",                                            
   randDist="UNIFORM",                                              
   scaleInit=1,                                                     
   seed=12345,                                                      
   std="MIDRANGE",                                                  

   validTable=vldTable,                                             

   casOut={                                                         
     "name":"Nnet_train_model",
     "replace":True
     },

   nloOpts={                                                        
     "lbfgsOpt":{"numCorrections":6},                               
     "optmlOpt":{                                                  
       "maxIters":250,                                              
       "fConv":1e-10                                                
       },
     "validate":{"frequency":1}}                                   
   )
 
