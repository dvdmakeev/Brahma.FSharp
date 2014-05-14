namespace AgentSystem

module Agents =

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

    type AgentsConfiguration =
        {
            Empty: unit
        }

    type GpuConfiguration =
        {
            DeviceType: DeviceType
            PlatformName: string
            CompileOptions: CompileOptions
            TrasnlatorOptions: TranslatorOption list
            OutCode: string ref
        }

    type IDataSource<'Data> =
        abstract member IsEnd: bool with get

    [<AbstractClass>]
    type Agent<'TaskParameter, 'TaskResult>(agentId: string, logger: ILogger) as this =
        interface IDisposable with
            member disposable.Dispose() = this.Dispose()

        member this.AgentId = agentId
        member this.Logger = logger
        
        abstract member Dispose: unit -> unit

        abstract member Post: Message<'TaskParameter, 'TaskResult> -> unit
        abstract member PostAndAsyncReply: 
            (AsyncReplyChannel<'Reply> -> Message<'TaskParameter, 'TaskResult>) -> Async<'Reply>

    and Message<'TaskParameter, 'TaskResult> =
    | DoTask of 'TaskParameter * AsyncReplyChannel<'TaskResult>
    | DataNeeded of AsyncReplyChannel<'TaskResult>
    | Data of 'TaskParameter * Agent<'TaskParameter, 'TaskResult>
    | Start 

    type AgentManager<'CpuTaskParameter, 'CpuTaskResult, 'GpuTaskParameter, 'GpuTaskResult, 'ReadingParameter, 'Data> =
        
        new (dataSource: IDataSource<'Data>) = {
        }

    type AgentDataReader<'ReadingParameter, 'Data> =
        inherit Agent<'ReadingParameter, 'Data>
        
        val dataReader: MailboxProcessor<Message<'ReadingParameter, 'Data>>

        new (agentId, logger, readFunction, parameters, dataSource: IDataSource<'Data>) = {
            inherit Agent<'ReadingParameter, 'Data>(agentId, logger)

            dataReader = MailboxProcessor.Start(fun inbox ->
                async {
                    while true do
                        logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
                        let! msg = inbox.Receive()

                        match msg with
                        | DataNeeded(reply) ->
                            logger.LogMessage(sprintf "%s recieved DataNeeded message at %s time" agentId currentDateTime)
                            reply.Reply(readFunction parameters dataSource)

                        | Start | DoTask(_, _) | Data(_, _) -> 
                            raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))

                })
        }

        override this.Post(msg) = this.dataReader.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> Message<'ReadingParameter, 'Data>) = 
            this.dataReader.PostAndAsyncReply buildMessage

        override this.Dispose() = (this.dataReader:> IDisposable).Dispose()

    type AgentCpuWorker<'TaskParameter, 'TaskResult> =
        inherit Agent<'TaskParameter, 'TaskResult>

        val worker: MailboxProcessor<Message<'TaskParameter, 'TaskResult>>
    
        new (agentId, logger, task) = {
            inherit Agent<'TaskParameter, 'TaskResult>(agentId, logger)
            
            worker = MailboxProcessor.Start(fun inbox ->
                async {
                    while true do
                        logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)
                        let! msg = inbox.Receive()
                        
                        match msg with
                        | DoTask(parameters, reply) -> 
                            logger.LogMessage(sprintf "%s recieved DoTask message at %s time" agentId currentDateTime)
                            reply.Reply(task parameters)
                            logger.LogMessage(sprintf "%s answered at %s time" agentId currentDateTime)
                            
                        | Start | DataNeeded(_) | Data(_, _) -> 
                            raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))
                }
            )
        }

        override this.Post(msg) = this.worker.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> Message<'TaskParameter, 'TaskResult>) = 
            this.worker.PostAndAsyncReply buildMessage

        override this.Dispose() = (this.worker:> IDisposable).Dispose()

    type AgentGpuWorker<'GpuTaskParameter, 'GpuTaskResult 
        when 'GpuTaskParameter: (new: unit -> 'GpuTaskParameter) and 
            'GpuTaskParameter: struct and 
            'GpuTaskParameter:> ValueType and 
            'GpuTaskParameter:> INDRangeDimension> =
        
        inherit Agent<'GpuTaskParameter, 'GpuTaskResult>

        val worker: MailboxProcessor<Message<'GpuTaskParameter, 'GpuTaskResult>>
        val provider: ComputeProvider
        val commandQueue: Brahma.OpenCL.CommandQueue

        new (agentId, logger, task, prePreparation: ('GpuTaskParameter -> unit) -> 'GpuTaskParameter -> unit, collectResults, gpuConfiguration) as this = {
            inherit Agent<'GpuTaskParameter, 'GpuTaskResult>(agentId, logger)

            provider = ComputeProvider.Create(gpuConfiguration.PlatformName, gpuConfiguration.DeviceType)
            commandQueue = new Brahma.OpenCL.CommandQueue(this.provider, this.provider.Devices |> Seq.head)

            worker = MailboxProcessor.Start(fun inbox ->
                async {
                    logger.LogMessage(sprintf "%s initialized at %s time" agentId currentDateTime)

                    let kernel, kernelPrepare, kernelRun = 
                        this.provider.Compile(task, gpuConfiguration.CompileOptions, gpuConfiguration.TrasnlatorOptions, gpuConfiguration.OutCode)
            
                    while true do
                        let! msg = inbox.Receive()

                        match msg with
                        | DoTask(parameters, reply) ->
                            prePreparation kernelPrepare parameters    
                            this.commandQueue.Add(kernelRun()).Finish() |> ignore
                            let results = collectResults this.commandQueue parameters this.provider
                            reply.Reply(results)

                        | Start | DataNeeded(_)| Data(_, _) -> raise (NotSupportedMessageException(sprintf "NotSupportedMessageException from %s" agentId))
                }
            )
        }

        override this.Post(msg) = this.worker.Post msg
        override this.PostAndAsyncReply(buildMessage: AsyncReplyChannel<'Reply> -> Message<'GpuTaskParameter, 'GpuTaskResult>) = 
            this.worker.PostAndAsyncReply buildMessage

        override this.Dispose() =
            this.commandQueue.Dispose()
            this.provider.CloseAllBuffers()
            (this.worker:> IDisposable).Dispose()