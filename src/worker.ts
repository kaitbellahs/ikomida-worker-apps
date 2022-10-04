import { Domain, Utils, DBModels, Types } from '@ikomida/shared-backend'
import { Message, Channel } from 'amqplib'
import { createRequire } from 'module'
const require = createRequire(import.meta.url)
let { name } = require('../package.json')
name = name
  .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
  .replace(/^\w/, (m: string) => m.toUpperCase())
  .replace(/-\w/g, (m: string[]) => m[1].toUpperCase())

class AppsWorker {
  googleAdmin?: Utils.GoogleAdmin
  appStoreConnect?: Utils.AppStoreConnect
  amqp?: Domain.RabbitMQ
  logger

  constructor() {
    this.logger = Utils.Logger.getInstance(name)
  }

  async run() {
    try {
      this.googleAdmin = new Utils.GoogleAdmin(this.logger)
      this.appStoreConnect = new Utils.AppStoreConnect(this.logger)
      this.amqp = new Domain.RabbitMQ(this.logger)
      await this.amqp.listenToMessages(Domain.RabbitMQ.APPS_QUEUE, this.processMessages.bind(this))
    } catch (error: any) {
      this.logger.error(error)
    }
  }

  async processMessages(message: Message, channel: Channel) {
    try {
      this.logger.log(` [x] ${message.fields?.routingKey}: payload received: '${message.content?.toString('utf8')}'`)
      const payloadObject: Types.Classes.CAMQPPayload<Types.Classes.CAMQPPayloadObject> =
        Types.Classes.CAMQPPayload.fromObject(JSON.parse(message.content?.toString('utf8') ?? '{}'))
      if (payloadObject.method === 'createApp') {
        const object = payloadObject?.object
        const payload: Types.Classes.CApp = Types.Classes.CApp.fromObject(object?.message)
        const platform = object?.platform
        const modelResponse = await this.model(payload, object?.contractId, platform)
        if (!modelResponse) {
          return false
        }
        const model = modelResponse as DBModels.AppModel
        let n = 0
        const startTime = new Date().getTime()
        let i = 0
        do {
          const response = await this.googleAdmin?.createNewApp(
            payload?.displayName,
            payload?.bundleId,
            platform ?? '-'
          )
          i++
          switch (response?.code) {
            case 0:
              model.fireBaseId = response?.id
              if (platform === 'ios') {
                const appStoreConnectResponse = payload?.bundleId
                  ? await this.appStoreConnect?.configureApp(payload?.bundleId)
                  : { code: -1 }
                if (appStoreConnectResponse?.code === 0) {
                  model.iOSProfileId = appStoreConnectResponse?.id
                }
              }
              await model.save()
              this.logger.log(` [x] App bundleId: ${model?.bundleId} platfrm: ${platform} foi criado com sucesso`)
              channel.ack(message)
              return true
            case 1:
              this.logger.warn(` [x] App bundleId: ${model?.bundleId} platfrm: ${platform} encontra-se criado`)
              channel.ack(message)
              return true
            case -1:
              if (i < 4) {
                n += i
                await Utils.System.sleep(n * 4000)
              }
              break
            default:
              return false
          }
        } while (i < 4)
        this.logger.error(
          `nao foi possivel o App bundleId: ${model?.bundleId} platfrm: ${platform} após ${i} tentativas em ${
            (startTime - new Date().getTime()) / 1000
          }s.`
        )
      }
    } catch (error: any) {
      this.logger.error(error)
    }
    channel.nack(message)
    return false
  }

  async model(object: Types.Classes.CApp, contractId?: string, platform?: string) {
    try {
      const contractModel = await DBModels.ContractModel.findOne({
        where: {
          id: contractId
        },
        include: {
          model: DBModels.AppModel,
          required: false,
          where: {
            platform
          }
        }
      })

      if (!contractModel) {
        return false
      }
      if (contractModel?.apps?.[0]) {
        return contractModel?.apps?.[0]
      }
      return contractModel.$create('app', {
        bundleId: object?.bundleId,
        displayName: object?.displayName,
        platform
      })
    } catch (error: any) {
      this.logger.error(error)
    }
    return null
  }
}

await new AppsWorker().run()
