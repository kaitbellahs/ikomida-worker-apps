#!/usr/bin/env node

import {
    RabbitMQ,
    SqlDB,
    GoogleAdmin,
    Logger,
    System
} from 'ikomida-shared';
import {
    createRequire
} from "module";
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

    googleAdmin;
    amqp;
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

    async processMessages(message, channel) {
        try {
            this.logger.log(` [x] ${message.fields.routingKey}: message received: '${message.content.toString('utf8')}'`)
            const messageObject = JSON.parse(message.content.toString('utf8'))
            if (messageObject.method === 'createApp') {
                const model = await createModel(messageObject.object.message, messageObject.object.contractId, messageObject.object.platform)
                for (let i = 1; i < 4; i++) {
                    if ((messageObject.object.platform === 'android' && await this.createAndroidApp(messageObject.object.message, model)) || (messageObject.object.platform === 'ios' && await this.createIosApp(messageObject.object.message, model))) {
                        break;
                    }
                    await System.sleep(i * 1000)
                }
            }
        } catch (error) {
            this.logger.error(error)
        } finally {
            channel.ack(message)
        }
    }

    async createModel(object, contractId, platform) {
        const contractModel = await SqlDB.ContractModel.findOne({
            where: {
                id: contractId
            }
        })

        if (!contractModel) {
            return false;
        }

        const appModel = await SqlDB.AppModel.create({
            bundleId: object?.bundleId,
            platform: platform,
            displayName: object?.displayName
        })

        await appModel.setContract(contractModel)
        return appModel;
    }

    async createAndroidApp(object, model) {
        const response = await this.googleAdmin.createNewAndroidApp(object.displayName, object.packageName)
        if (!response) {
            return false;
        }
        model.fireBase = true;
        model.fireBaseId = response;
        model.save()
        return true;
    }

    async createIosApp(object, model) {
        const response = await this.googleAdmin.createNewIosApp(object.displayName, object.packageName)
        if (!response) {
            return false;
        }
        model.fireBase = true;
        model.fireBaseId = response;
        model.save()
        return true;
    }
}

await (new AppsWorker).run()