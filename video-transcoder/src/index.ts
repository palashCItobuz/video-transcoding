import { SQSClient, ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { S3Client, GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import type { S3Event } from 'aws-lambda';
import fs from "fs";
import fetch from "node-fetch";
import type { Readable } from 'stream';

const AWS_REGION = "ap-south-1";
const AWS_BUCKET_NAME = "sehncoded";

const sqsClient = new SQSClient({ region: AWS_REGION });
const s3 = new S3Client({ region: AWS_REGION });

const init = async () => {
  const command = new ReceiveMessageCommand({
    QueueUrl: 'https://sqs.ap-south-1.amazonaws.com/034014525464/s3Q-zing',
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20, // Enable long polling
  });

  while (true) {
    try {
      console.log('Polling for messages...');
      await pollMessages(command);
    } catch (error) {
      console.error('Error while polling messages:', error);
    }
  }
};

const pollMessages = async (command: ReceiveMessageCommand) => {
  const { Messages } = await sqsClient.send(command);
  if(!Messages || Messages.length === 0) {
    console.log('No messages received, polling again...');
    return;
  }
  for (const message of Messages) {
    const { Body } = message;
    if (!Body) continue;

    const event = JSON.parse(Body) as S3Event;
    // Process the message here
    if("Service" in event && "Event" in event) {
      if(event.Event === "s3:TestEvent") continue;
    }
    // handleMessage(message)
    await handleEvent(event);
  }
};

async function handleEvent(event: S3Event) {
  for (const record of event.Records) {
    const { s3 } = record;
    const { bucket, object: { key } } = s3;
    
  }
};

async function handleMessage(message: any) {
  const body = JSON.parse(message.Body);
  const key = decodeURIComponent(body.Records[0].s3.object.key.replace(/\+/g, " "));
 
  const outputFile = `/tmp/output-${Date.now()}.mp4`;

  console.log(`Downloading s3://${AWS_BUCKET_NAME}/${key}`);
  await downloadVideo(key, outputFile);

  console.log("Transcoding...");
  //await transcodeVideo(inputFile, outputFile);

  console.log("✅ Done");
};

async function downloadVideo(key: string, outputPath: string) {
  /* const command = new GetObjectCommand({
    Bucket: AWS_BUCKET_NAME,
    Key: key
  });

  const response = await s3.send(command); 
  if (!response) throw new Error(`Download failed: ${response}`);

  console.log(response.Body)

  const bodyStream = response.Body as Readable;
  await new Promise<void>((resolve, reject) => {
    const writeStream = fs.createWriteStream(outputPath);
    bodyStream.pipe(writeStream);
    bodyStream.on('error', reject);
    writeStream.on('finish', resolve);
  }); */

  try {
    console.log("tmp exists?", fs.existsSync("/container/tmp"));
    const obj = await s3.send(new HeadObjectCommand({ Bucket: AWS_BUCKET_NAME, Key: key }));
    const command = new GetObjectCommand({
      Bucket: AWS_BUCKET_NAME,
      Key: key
    });

    const response = await s3.send(command);
    const bodyStream = response.Body as Readable;
    await new Promise<void>((resolve, reject) => {
      const writeStream = fs.createWriteStream(outputPath);
      bodyStream.pipe(writeStream);
      bodyStream.on('error', reject);
      writeStream.on('finish', resolve);
    });
    const stats = fs.statSync(outputPath);
    console.log(`Downloaded to ${outputPath}, local file size: ${stats.size} bytes`);
    console.log("File exists. Size:", obj.ContentLength);
  } catch (err: any) {
    console.error(`❌ File does not exist or permission denied for ${key}`, err.name, err.message);
    return;
  }
};

init();