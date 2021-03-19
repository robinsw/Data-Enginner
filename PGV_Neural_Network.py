 import swat                                                       #1

 #  Specify the host and port information match your site

 s = swat.CAS("cloud.example.com", 8561)

 from IPython.core.display import display                          # 2

 result = s.table.upload(
   path="/home/viya/PGV.csv",
   casout={
     "name":"PGV",
     "replace":True}                                               # 3
   )

 PGV = s.CASTable(result.tableName)                               # 4


 # Display Table Info for PGV

 ti = PGV.table.tableInfo().TableInfo                             # 5
 display(HTML('<h3>Table Information</h3>'))
 display(ti.ix[:,'Name':'JavaCharSet'])
 display(ti.ix[:,'ModTime':])



 # Display Table Details for PGV

 td = PGV.table.tableDetails().TableDetails                       # 6
 display(HTML('<h3>Table Details</h3>'))
 display(td.ix[:,'Node':'VardataSize'])
 display(td.ix[:,'Mapped':])



 # Use the sampling_Stratified action to partition the PGV input
 # data by variable SPECIES into training (70%) and validation
 # (30%) tables.

 result = s.sampling.stratified(                                    # 7
      table={
     "name":"PGV",
     "groupby":[{
       "name":"species"}]},

     partind=True,                                                  # 8

   output={                                                         # 9
     "casout":{
       "name":"PGV_partitioned",
       "replace":True},
       "copyVars":"ALL",
       "partindname":"Partn_Val"                                    # 10
       },

   samppct=70,                                                      # 11
   seed=12345)                                                      # 12


 # Use the map data in the newly added Partn_Val column to create
 # separate CAS tables for Neural Net training and validation.

 trnTable={                                                         # 13
   "name":"PGV_partitioned",
   "where":"1=Partn_Val"}

 vldTable={                                                         # 14
   "name":"PGV_partitioned",
   "where":"0=Partn_Val"}

 -- Use the annTrain action to create and train a MLP neural network
 -- for a nominal target SPECIES.

 results = s.neuralNet.annTrain(                                    # 15
   table=trnTable,
   target="species",

   "nominals"=[{                                                    # 16
     "name":"species"}],

   inputs=[{                                                        # 17
     "name":{
       "sepallength",
       "sepalwidth",
       "petallength",
       "petalwidth"}}
     ],
   listNode="ALL",                                                  # 18
   arch="MLP",                                                      # 19
   hiddens={2},                                                     # 20
   combs={"LINEAR"},                                                # 21
   targetAct="SOFTMAX",                                             # 22
   errorFunc="ENTROPY",                                             # 23
   randDist="UNIFORM",                                              # 24
   scaleInit=1,                                                     # 25
   seed=12345,                                                      # 26
   std="MIDRANGE",                                                  # 27

   validTable=vldTable,                                             # 28

   casOut={                                                         # 29
     "name":"Nnet_train_model",
     "replace":True
     },

   nloOpts={                                                        # 30
     "lbfgsOpt":{"numCorrections":6},                               # 31
     "optmlOpt":{                                                   # 32
       "maxIters":250,                                              # 33
       "fConv":1e-10                                                # 34
       },
     "validate":{"frequency":1}}                                    # 35
   )
 