#!/usr/bin/env node

import {
    RabbitMQ,
    MariaDB,
    GoogleAdmin,
    Logger
} from 'ikomida-shared';
import {
    createRequire
} from "module";
const require = createRequire(
    import.meta.url);
let {
    name
} = require("../package.json");
name = name
    .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
    .replace(/^\w/, m => m.toUpperCase())
    .replace(/-\w/g, m => m[1].toUpperCase());
const logger = Logger.getInstance(name, process.env?.ENV !== 'PROD');

class AppsWorker {

    googleAdmin;
    amqp;

    //TODO: -- report errors
    async run() {
        try {
            this.googleAdmin = new GoogleAdmin();
            this.amqp = new RabbitMQ(logger);
            await this.amqp.listenToMessages(RabbitMQ.APPS_SEVERITY, this.processMessages.bind(this));
        } catch (error) {
            console.error(error);
        }
    }

    async processMessages(message, channel) {
        try {
            console.log(" [x] %s: message received: '%s'", message.fields.routingKey, message.content.toString('utf8'));
            const messageObject = JSON.parse(message.content.toString('utf8'));
            if (messageObject.method === 'createApp') {
                const model = createModel(messageObject.object.message, messageObject.object.contractId, messageObject.object.platform);
                for (let i = 1; i < 4; i++) {
                    if ((messageObject.object.platform === 'android' && this.createAndroidApp(messageObject.object.message, model)) || (messageObject.object.platform === 'ios' && this.createIosApp(messageObject.object.message, model))) {
                        break;
                    }
                    await this.sleep(i * 1000);
                }
            }
        } catch (error) {
            console.error(error);
        } finally {
            channel.ack(message);
        }
    }

    async createModel(object, contractId, platform) {
        const contractModel = await MariaDB.ContractModel.findOne({
            where: {
                id: contractId
            }
        });

        if (!contractModel) {
            return false;
        }

        const appModel = await MariaDB.AppModel.create({
            bundleId: object?.bundleId,
            platform: platform,
            displayName: object?.displayName
        });

        await appModel.setContract(contractModel);
        return appModel;
    }

    async createAndroidApp(object, model) {
        const response = await this.googleAdmin.createNewAndroidApp(object.displayName, object.packageName);
        console.log(response);
        if (!response) {
            return false;
        }
        model.fireBase = true;
        model.fireBaseId = response;
        model.save();
        return true;
    }

    async createIosApp(object, model) {
        const response = await this.googleAdmin.createNewIosApp(object.displayName, object.packageName);
        console.log(response);
        if (!response) {
            return false;
        }
        model.fireBase = true;
        model.fireBaseId = response;
        model.save();
        return true;
    }

    async sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

await (new AppsWorker).run();