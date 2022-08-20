#!/usr/bin/env node

import {
    RabbitMQ,
    SqlDB,
    GoogleAdmin,
    Logger,
    System
} from 'ikomida-shared'
import {
    createRequire
} from "module"
const require = createRequire(
    import.meta.url)
let {
    name
} = require("../package.json")
name = name
    .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
    .replace(/^\w/, m => m.toUpperCase())
    .replace(/-\w/g, m => m[1].toUpperCase())

class AppsWorker {

    googleAdmin
    amqp
    logger

    constructor() {
        this.logger = Logger.getInstance(name, process.env?.ENV !== 'PROD')
    }

    async run() {
        try {
            this.googleAdmin = new GoogleAdmin(this.logger)
            this.amqp = new RabbitMQ(this.logger)
            await this.amqp.listenToMessages(RabbitMQ.APPS_QUEUE, this.processMessages.bind(this))
        } catch (error) {
            this.logger.error(error)
        }
    }

    async processMessages(payload, channel) {
        try {
            this.logger.log(` [x] ${payload?.fields?.routingKey}: message received: '${payload?.content?.toString('utf8')}'`)
            const messageObject = JSON.parse(payload?.content?.toString('utf8'))
            if (messageObject.method === 'createApp') {
                const message = messageObject?.object?.message
                const platform = messageObject?.object?.platform
                const model = await this.createModel(message, messageObject.object?.contractId, platform)
                if (!model) {
                    return false
                }
                let n = 0
                let total = 0
                let i = 0
                do {
                    let response
                    if (platform === 'android') {
                        response = await this.googleAdmin?.createNewAndroidApp(message?.displayName, message?.packageName)
                    }
                    if (platform === 'ios') {
                        response = await this.googleAdmin?.createNewIosApp(message?.displayName, message?.packageName)
                    }
                    i++
                    switch (response?.code) {
                        case 0:
                            model.fireBase = true
                            model.fireBaseId = response?.id
                            await model.save()
                            this.logger.log(` [x] App bundleId: ${model?.bundleId} platfrm: ${platform} foi criado com sucesso`)
                            channel.ack(payload)
                            return true
                        case 1:
                            this.logger.warn(` [x] App bundleId: ${model?.bundleId} platfrm: ${platform} encontra-se criado`)
                            channel.ack(payload)
                            return true
                        case -1:
                            if (i < 4) {
                                n += i
                                total += n * 4
                                await System.sleep(n * 4000)
                            }
                            break
                        default:
                            return false
                    }
                } while (i < 4)
                this.logger.error(`nao foi possivel o App bundleId: ${model?.bundleId} platfrm: ${platform} após ${i} tentativas em ${total}s.`)
            }
        } catch (error) {
            this.logger.error(error)
        }
        return false
    }

    async createModel(object, contractId, platform) {
        try {
            const contractModel = await SqlDB.ContractModel.findOne({
                where: {
                    id: contractId
                }
            })

            if (!contractModel) {
                return false
            }

            const appModel = await contractModel.createApp({
                bundleId: object?.packageName,
                platform: platform,
                displayName: object?.displayName
            })
            return appModel
        } catch (error) {
            this.logger.error(error)
        }
        return null
    }
}

await (new AppsWorker).run()