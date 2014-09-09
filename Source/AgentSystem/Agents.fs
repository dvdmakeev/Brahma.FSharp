namespace BrahmaAgents

module AgentsBase =

    open System
    open System.Collections.Generic
    open System.Linq
    open Microsoft.FSharp.Quotations

    open AgentUtils
    open Brahma.OpenCL
    open Brahma.FSharp.OpenCL.Core;
    open Brahma.FSharp.OpenCL.Translator.Common
    open OpenCL.Net

    exception NotSupportedMessageException of string

    let currentDateTime = DateTime.Now.ToString()

    type AgentType = 
        | CpuWorker
        | GpuWorker
        | DataReader
        | Manager

    type AgentsOverallConfiguration =
        {
            AgentCpuWorkersCount: int
            
            AgentGpuWorkersCount: int
            AgentDataReadersCount: int
        }

    type AgentCpuWorkerConfiguration =
        {
            Empty: unit
        }

    type AgentGpuWorkerConfiguration =
        {
            Empty: unit
        }

    type AgentDataReaderConfiguration<'ReadingParameter> =
        {
            ReadingParameter: 'ReadingParameter
        }

    type GpuConfiguration =
        {
            DeviceType: DeviceType
            PlatformName: string
//            CompileOptions: CompileOptions
//            TrasnlatorOptions: TranslatorOption list
//            OutCode: string ref
        }

    type IDataSource<'Data> =
        abstract member IsEnd: bool with get

    type IMessage = interface end

    [<AbstractClass>]
    type Agent<'MessageType>(agentId: string, logger: ILogger) as this =
        interface IDisposable with
            member disposable.Dispose() = this.Dispose()

        member this.AgentId = agentId
        member this.Logger = logger
        
        abstract member Dispose: unit -> unit

        abstract member AgentType: AgentType with get

        abstract member Post: 'MessageType -> unit
        abstract member PostAndAsyncReply: 
            (AsyncReplyChannel<'Reply> -> 'MessageType) -> Async<'Reply>

    and MessageCpuWorker<'CpuTaskParameter, 'CpuTaskResult> =
        | DoCpuTask of 'CpuTaskParameter
        
        interface IMessage

    and MessageGpuWorker<'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult> =
        
        | DoGpuTask of 'GpuTaskNecessaryParameter * 'GpuTaskAdditionalParameter
        
        interface IMessage

    and MessageDataReader<'ReadingParameter, 'Data> =
        | DataNeeded of 'ReadingParameter

        interface IMessage

    and MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                        'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                        'ReadingParameter, 'Data,
                        'ManagerParams, 'OverallResult
                            when 'GpuTaskNecessaryParameter: (new: unit -> 'GpuTaskNecessaryParameter) and
                                    'GpuTaskNecessaryParameter: struct and
                                    'GpuTaskNecessaryParameter:> ValueType and
                                    'GpuTaskNecessaryParameter:> INDRangeDimension> =

        | Start of 'ManagerParams

        | CpuTaskDone of 'CpuTaskResult * 
            AgentCpuWorker<'CpuTaskParameter, 'CpuTaskResult,
                            'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                            'ReadingParameter, 'Data,
                            'ManagerParams, 'OverallResult>

        | GpuTaskDone of 'GpuTaskResult * 
            AgentGpuWorker<'CpuTaskParameter, 'CpuTaskResult,
                            'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                            'ReadingParameter, 'Data,
                            'ManagerParams, 'OverallResult>

        | DataFromReader of 'Data * 
            AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,
                            'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                            'ReadingParameter, 'Data,
                            'ManagerParams, 'OverallResult>

        | Completed

        interface IMessage

    and AgentCpuWorker<'CpuTaskParameter, 'CpuTaskResult,
                        'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                        'ReadingParameter, 'Data,
                        'ManagerParams, 'OverallResult
                            when 'GpuTaskNecessaryParameter: (new: unit -> 'GpuTaskNecessaryParameter) and
                                'GpuTaskNecessaryParameter: struct and
                                'GpuTaskNecessaryParameter:> ValueType and
                                'GpuTaskNecessaryParameter:> INDRangeDimension> =
        
        inherit Agent<MessageCpuWorker<'CpuTaskParameter, 'CpuTaskResult>>
        
        val worker: MailboxProcessor<MessageCpuWorker<'CpuTaskParameter, 'CpuTaskResult>>
        
        new (
            agentId, 
            logger, 
            task: 'CpuTaskParameter -> 'CpuTaskResult, 
            resultReceiver: Agent<MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                                                     'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                                                     'ReadingParameter, 'Data,
                                                     'ManagerParams, 'OverallResult>>) as this = {
            
            inherit Agent<MessageCpuWorker<'CpuTaskParameter, 'CpuTaskResult>>(agentId, logger)
           
            worker = MailboxProcessor.Start(fun inbox ->
                async {
                    logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
                    
                    while true do
                        let! msg = inbox.Receive()
                        
                        match msg with
                        | DoCpuTask(parameters) -> 
                            logger.LogMessage(sprintf "%s recieved DoTask message at %s time" agentId currentDateTime)
                            resultReceiver.Post(CpuTaskDone((task parameters), this))
                            logger.LogMessage(sprintf "%s answered at %s time" agentId currentDateTime)
                            
                        | _ -> 
                            raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))
                }
            )
        }
        
        override this.AgentType with get() = AgentType.CpuWorker
        
        override this.Post(msg) = this.worker.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> MessageCpuWorker<'CpuTaskParameter, 'CpuTaskResult>) = 
            this.worker.PostAndAsyncReply buildMessage
        
        override this.Dispose() = (this.worker:> IDisposable).Dispose()

    and AgentGpuWorker<'CpuTaskParameter, 'CpuTaskResult,
                        'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                        'ReadingParameter, 'Data,
                        'ManagerParams, 'OverallResult
                            when 'GpuTaskNecessaryParameter: (new: unit -> 'GpuTaskNecessaryParameter) and
                                'GpuTaskNecessaryParameter: struct and
                                'GpuTaskNecessaryParameter:> ValueType and
                                'GpuTaskNecessaryParameter:> INDRangeDimension> =
        
        inherit Agent<MessageGpuWorker<'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult>>
        
        val worker: MailboxProcessor<MessageGpuWorker<'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult>>
        val provider: ComputeProvider
        val commandQueue: Brahma.OpenCL.CommandQueue
        
        new (
            agentId, 
            logger, 
            task, 
            prePreparation: ('GpuTaskNecessaryParameter * 'GpuTaskAdditionalParameter -> unit) -> 'GpuTaskNecessaryParameter * 'GpuTaskAdditionalParameter -> unit, 
            collectResults, 
            gpuConfiguration,
            resultReceiver: Agent<MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                                                     'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                                                     'ReadingParameter, 'Data,
                                                     'ManagerParams, 'OverallResult>>) as this = 
                                                     
            let initializedProvider = ComputeProvider.Create(gpuConfiguration.PlatformName, gpuConfiguration.DeviceType)
            {            
                inherit Agent<MessageGpuWorker<'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult>>(agentId, logger)
            
                provider = initializedProvider
            
                commandQueue = new Brahma.OpenCL.CommandQueue(initializedProvider, initializedProvider.Devices |> Seq.head)
        
                worker = MailboxProcessor.Start(fun inbox ->
                    async {
                        logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
        
                        let kernel, kernelPrepare, kernelRun = this.provider.Compile(task)
                    
                        while true do
                            let! msg = inbox.Receive()
                            match msg with
                            | DoGpuTask(parameters, additionalParams) ->
                                kernelPrepare parameters additionalParams
                                this.commandQueue.Add(kernelRun()).Finish() |> ignore
                                let results = collectResults this.commandQueue parameters additionalParams this.provider
                                resultReceiver.Post(GpuTaskDone(results, this))
                            
                            | _ -> raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))
                    }
                )
            }

        override this.AgentType with get() = AgentType.GpuWorker
        
        override this.Post(msg) = this.worker.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> MessageGpuWorker<'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult>) = 
            this.worker.PostAndAsyncReply buildMessage
        
        override this.Dispose() =
            this.commandQueue.Dispose()
            this.provider.CloseAllBuffers()
            (this.worker:> IDisposable).Dispose()

    and AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,
                         'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                         'ReadingParameter, 'Data,
                         'ManagerParams, 'OverallResult
                             when 'GpuTaskNecessaryParameter: (new: unit -> 'GpuTaskNecessaryParameter) and
                                 'GpuTaskNecessaryParameter: struct and
                                 'GpuTaskNecessaryParameter:> ValueType and
                                 'GpuTaskNecessaryParameter:> INDRangeDimension> =

        inherit Agent<MessageDataReader<'ReadingParameter, 'Data>>
        
        val dataReader: MailboxProcessor<MessageDataReader<'ReadingParameter, 'Data>>
        
        new (
            agentId, 
            logger, 
            readFunction, 
            parameters: 'ReadingParameter, 
            dataSource: IDataSource<'Data>,
            dataReceiver: Agent<MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                                                     'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                                                     'ReadingParameter, 'Data,
                                                     'ManagerParams, 'OverallResult>>) as this = {
            
            inherit Agent<MessageDataReader<'ReadingParameter, 'Data>>(agentId, logger)
            
            dataReader = MailboxProcessor.Start(fun inbox ->
                async {
                    logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
                    
                    while true do
                        let! msg = inbox.Receive()
        
                        match msg with
                        | DataNeeded(param) ->
                            logger.LogMessage(sprintf "%s recieved DataNeeded message at %s time" agentId currentDateTime)
                            
                            if dataSource.IsEnd 
                            then
                                dataReceiver.Post(Completed)
                            else
                                dataReceiver.Post(DataFromReader((readFunction parameters dataSource), this))
        
                        | _ -> 
                            raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))
        
                }
            )
        }
        
        override this.AgentType with get() = AgentType.DataReader
        
        override this.Post(msg) = this.dataReader.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> MessageDataReader<'ReadingParameter, 'Data>) = 
            this.dataReader.PostAndAsyncReply buildMessage
        
        override this.Dispose() = (this.dataReader:> IDisposable).Dispose()

    and AgentManager<'CpuTaskParameter, 'CpuTaskResult,
                        'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                        'ReadingParameter, 'Data,
                        'ManagerParams, 'OverallResult
                            when 'GpuTaskNecessaryParameter: (new: unit -> 'GpuTaskNecessaryParameter) and
                                'GpuTaskNecessaryParameter: struct and
                                'GpuTaskNecessaryParameter:> ValueType and
                                'GpuTaskNecessaryParameter:> INDRangeDimension> =
        
        inherit Agent<MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                                     'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                                     'ReadingParameter, 'Data,
                                     'ManagerParams, 'OverallResult>>
        
        val manager: MailboxProcessor<MessageManager<'CpuTaskParameter, 'CpuTaskResult,
                                                     'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,
                                                     'ReadingParameter, 'Data,
                                                     'ManagerParams, 'OverallResult>>
        
        val dataPool: DataPool<'Data>

        val mutable cpuWorkersCount: int
        val mutable gpuWorkersCount: int
        val mutable dataReadersCount: int
        
        new (
            agentId, 
            logger, 
            overallConfigs: AgentsOverallConfiguration,
            cpuTask: 'CpuTaskParameter -> 'CpuTaskResult,
            gpuTask, 
            prePreparation,
            collectResults,
            gpuConfiguration, 
            readFunction,
            readingParam,
            dataToCpuTaskParams: 'Data -> 'CpuTaskParameter,
            dataToGpuTaskParams: 'Data -> 'GpuTaskNecessaryParameter * 'GpuTaskAdditionalParameter,
            managerParams,
            dataSource: IDataSource<'Data>) as this = {
            
            inherit Agent<MessageManager<'CpuTaskParameter, 'CpuTaskResult, 'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>>(agentId, logger)
        
            cpuWorkersCount = 0
            gpuWorkersCount = 0
            dataReadersCount = 0
            dataPool = new DataPool<'Data>()

            manager = MailboxProcessor.Start(fun inbox ->
                async {
                    let hasWork = ref true
                    
                    logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
        
                    let cpuTaskResults = new List<'CpuTaskResult>()
                    let gpuTaskResults = new List<'GpuTaskResult>()

                    let freeCpuWorkers: List<AgentCpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,'ReadingParameter, 'Data,'ManagerParams, 'OverallResult>> = 
                        this.CreateCpuWorkers(overallConfigs.AgentCpuWorkersCount, cpuTask)
                    
                    let freeGpuWorkers: List<AgentGpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,'ReadingParameter, 'Data,'ManagerParams, 'OverallResult>> = 
                        this.CreateGpuWorkers(
                            overallConfigs.AgentGpuWorkersCount, 
                            gpuTask, 
                            prePreparation, 
                            collectResults,
                            gpuConfiguration)
                    
                    let freeDataReaders: List<AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,'ReadingParameter, 'Data,'ManagerParams, 'OverallResult>> = 
                        this.CreateDataReaders(
                            overallConfigs.AgentDataReadersCount, 
                            readFunction, 
                            readingParam, 
                            dataSource)
        
                    let startRead (reader: AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult,'ReadingParameter, 'Data,'ManagerParams, 'OverallResult>) = 
                        async { return reader.Post(MessageDataReader<'ReadingParameter, 'Data>.DataNeeded(readingParam)) }
                    
                    while !hasWork do
                        let! msg = inbox.Receive()
        
                        match msg with
                        | Start(managerParams) -> 
                            logger.LogMessage(sprintf "%s recieved Start message at %s time" agentId currentDateTime)
                            logger.LogMessage "Starting Readers"
                          
                            let! startAllReaders = async { freeDataReaders |> Seq.map startRead |> Async.Parallel |> Async.RunSynchronously |> ignore }
                            
                            logger.LogMessage "Readers started"

                        | DataFromReader(data, reader) -> 
                            freeDataReaders.Add reader
                            this.dataPool.AddData(data)
                            
                        | CpuTaskDone(taskRes, worker) ->
                            logger.LogMessage(
                                sprintf "%s recieved CpuTaskDone message from %s at %s time" agentId worker.AgentId currentDateTime)
                            
                            cpuTaskResults.Add taskRes
                            worker.Post(DoCpuTask((dataToCpuTaskParams (this.dataPool.GetData()))))
                            
                            let reader = freeDataReaders.[freeDataReaders.Count - 1]
                            reader.Post(DataNeeded(readingParam))
                                
                        | GpuTaskDone(taskRes, worker) ->
                            logger.LogMessage(
                                sprintf "%s recieved GpuTaskDone message from %s at %s time" agentId worker.AgentId currentDateTime)
                            
                            gpuTaskResults.Add taskRes
                            worker.Post(DoGpuTask((dataToGpuTaskParams (this.dataPool.GetData()))))

                            let reader = freeDataReaders.[freeDataReaders.Count - 1]
                            reader.Post(DataNeeded(readingParam))

                        | Completed ->
                            logger.LogMessage(sprintf "%s recieved Completed message at %s time" agentId currentDateTime)
                            hasWork := false
                }
            )
        }
        
        override this.AgentType with get() = AgentType.Manager
        
        override this.Post(msg) = this.manager.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> MessageManager<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>) = 
            this.manager.PostAndAsyncReply buildMessage
        
        override this.Dispose() = (this.manager:> IDisposable).Dispose()
        
        member private this.CreateCpuWorkers(agentsCount, task) =
            let workers = new List<AgentCpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>>()
            
            let mutable i = 0
            while i < agentsCount do
                workers.Add(new AgentCpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>(this.CreateAgentId(CpuWorker), this.Logger, task, this))
                this.cpuWorkersCount <- this.cpuWorkersCount + 1
                i <- i + 1
        
            workers
        
        member private this.CreateGpuWorkers(agentsCount, task, prePreparation, collectResults, configs) =
            let workers = new List<AgentGpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>>()
            
            let mutable i = 0
            while i < agentsCount do
                workers.Add(new AgentGpuWorker<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>(this.CreateAgentId(GpuWorker), this.Logger, task, prePreparation, collectResults, configs, this))
                this.gpuWorkersCount <- this.gpuWorkersCount + 1
                i <- i + 1
        
            workers
        
        member private this.CreateDataReaders(agentsCount, readFunction, parameters, dataSource) =
            let dataReaders = new List<AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>>()
            
            let mutable i = 0
            while i < agentsCount do
                dataReaders.Add(new AgentDataReader<'CpuTaskParameter, 'CpuTaskResult,'GpuTaskNecessaryParameter, 'GpuTaskAdditionalParameter, 'GpuTaskResult, 'ReadingParameter, 'Data, 'ManagerParams, 'OverallResult>(this.CreateAgentId(DataReader), this.Logger, readFunction, parameters, dataSource, this))
                this.dataReadersCount <- this.dataReadersCount + 1
                i <- i + 1
        
            dataReaders
        
        member private this.CreateAgentId(agentType: AgentType) =
            match agentType with
                | CpuWorker -> sprintf "%dCpuWorker" this.cpuWorkersCount
                | GpuWorker -> sprintf "%dGpuWorker" this.gpuWorkersCount
                | DataReader -> sprintf "%dDataReader" this.dataReadersCount
                | _ -> raise (NotSupportedMessageException(""))