import mqtt from 'mqtt'
import type { MqttClient, IClientOptions } from 'mqtt'
import type { MqttConnectionOptions } from '../types.js'

export function parseUndefined<T extends Record<string, any>>(obj: T): Partial<T> {
  const result = {} as Partial<T>
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) {
      result[key as keyof T] = value
    }
  }
  return result
}

export interface MqttAdapter {
  connect(): Promise<void>
  disconnect(): Promise<void>
  subscribe(topic: string, options?: { nl?: boolean }): Promise<void>
  unsubscribe(topic: string): Promise<void>
  publish(
    topic: string,
    message: string,
    options?: {
      qos?: 0 | 1 | 2
      retain?: boolean
      userProperties?: Record<string, string>
    },
  ): Promise<void>
  on(event: 'message', callback: (topic: string, payload: Buffer, packet?: any) => void): void
  on(event: 'connect' | 'disconnect' | 'error', callback: (...args: any[]) => void): void
  isConnected(): boolean
  getConnackProperties(): Record<string, any> | undefined
  getClient(): MqttClient | null
}

export class UniversalMqttAdapter implements MqttAdapter {
  private client: MqttClient | null = null
  private options: MqttConnectionOptions
  private connackProperties: Record<string, any> | undefined

  constructor(options: MqttConnectionOptions) {
    this.options = options
  }

  private buildMqttOptions(): IClientOptions {
    return {
      ...parseUndefined({
        clientId: this.options.clientId,
        username: this.options.username,
        password: this.options.password,
        will: this.options.will,
        properties: this.options.properties,
      }),
      clean: this.options.clean ?? true,
      keepalive: this.options.keepalive ?? 60,
      connectTimeout: this.options.connectTimeout ?? 30000,
      reconnectPeriod: this.options.reconnectPeriod ?? 1000,
      protocolVersion: 5, // Force MQTT 5.0
      properties: {
        sessionExpiryInterval: 0, // As required by specification
        ...this.options.properties,
      },
    }
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        const options = this.buildMqttOptions()
        this.client = mqtt.connect(this.options.host, options)

        let connected = false
        this.client.on('connect', (connack) => {
          // Store CONNACK properties for later access
          this.connackProperties = connack?.properties
          connected = true
          resolve()
        })

        this.client.on('error', (error) => {
          reject(error)
        })

        this.client.on('close', () => {
          // closed but not connected
          if (!connected) {
            reject(new Error('Connection closed'))
          }
        })

        // Set a connection timeout
        const timeout = setTimeout(() => {
          reject(new Error('Connection timeout'))
        }, this.options.connectTimeout ?? 30000)

        this.client.on('connect', () => {
          clearTimeout(timeout)
        })
      } catch (error) {
        reject(error)
      }
    })
  }

  async disconnect(): Promise<void> {
    return new Promise((resolve) => {
      if (this.client) {
        this.client.end(true, {}, () => {
          this.client = null
          resolve()
        })
      } else {
        resolve()
      }
    })
  }

  async subscribe(topic: string, options?: { nl?: boolean }): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error('MQTT client not connected'))
        return
      }

      const subscribeOptions: any = {
        qos: 1, // Default QoS
      }

      // Add No Local option for MQTT 5.0
      if (options?.nl) {
        subscribeOptions.nl = true // No Local flag
      }

      this.client.subscribe(topic, subscribeOptions, (error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  async unsubscribe(topic: string): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error('MQTT client not connected'))
        return
      }

      this.client.unsubscribe(topic, (error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  async publish(
    topic: string,
    message: string,
    options?: {
      qos?: 0 | 1 | 2
      retain?: boolean
      userProperties?: Record<string, string>
    },
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error('MQTT client not connected'))
        return
      }

      const publishOptions: any = {
        qos: options?.qos ?? 1, // Default QoS 1
        retain: options?.retain ?? false,
      }

      // Add MQTT 5.0 user properties
      if (options?.userProperties) {
        publishOptions.properties = {
          userProperties: options.userProperties,
        }
      }

      this.client.publish(topic, message, publishOptions, (error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  on(event: 'message', callback: (topic: string, payload: Buffer, packet?: any) => void): void
  on(event: 'connect' | 'disconnect' | 'error', callback: (...args: any[]) => void): void
  on(event: string, callback: (...args: any[]) => void): void {
    if (!this.client) {
      throw new Error('MQTT client not initialized')
    }

    // Special handling for message event to pass packet info
    if (event === 'message') {
      this.client.on('message', (topic: string, payload: Buffer, packet: any) => {
        callback(topic, payload, packet)
      })
    } else {
      this.client.on(event as any, callback)
    }
  }

  isConnected(): boolean {
    return this.client?.connected ?? false
  }

  getConnackProperties(): Record<string, any> | undefined {
    return this.connackProperties
  }

  getClient(): MqttClient | null {
    return this.client
  }
}
