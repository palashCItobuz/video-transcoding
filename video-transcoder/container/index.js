const { S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const ffmpeg = require("fluent-ffmpeg");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const stream = require("node:stream");
const { promisify } = require("node:util");


const AWS_REGION = "ap-south-1";
const AWS_TRANSCODED_BUCKET_NAME = "sehncoded-transcoded-files";
const s3Client = new S3Client({ region: AWS_REGION });
const AWS_BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const key = process.env.KEY;
const pipeline = promisify(stream.pipeline);

const RESOLUTIONS = [
  { name: "360p", width: 480, height: 360 },
  { name: "480p", width: 858, height: 480 },
  { name: "720p", width: 1280, height: 720 }
];

async function init() {
  try {
    console.log("tmp exists?", fs.existsSync("/tmp"));
    const obj = await s3Client.send(new HeadObjectCommand({ Bucket: AWS_BUCKET_NAME, Key: key }));
    const command = new GetObjectCommand({
      Bucket: AWS_BUCKET_NAME,
      Key: key
    });
    const match = key.match(/(?<=uploads\/).*?(?=\.mp4)/);
    const response = await s3Client.send(command);
    const originalPath = `tmp/${match[0]}-video.mp4`;
    await pipeline(response.Body, fs.createWriteStream(originalPath));
    //await fsp.writeFile(originalPath, response.Body);
    
    const promises = RESOLUTIONS.map(resolution => {
      const outputPath = `videos/${match[0]}-${resolution.name}-video.mp4`;
      return new Promise((resolve, reject) => {
        ffmpeg(originalPath)
          .output(outputPath)
          .withVideoCodec('libx264')
          .withAudioCodec('aac')
          .withSize(`${resolution.width}x${resolution.height}`)
          .format('mp4')
          .on('end', () => {
            const fileStream = fs.createReadStream(outputPath);
            s3Client.send(new PutObjectCommand({
              Bucket: AWS_TRANSCODED_BUCKET_NAME,
              Key: `transcoded/${match[0]}-${resolution.name}-video.mp4`,
              Body: fileStream,
              ContentType: 'video/mp4'
            }));
            console.log(`Transcoding to ${resolution.name} completed`);
            resolve();
          })
          .on('error', (err) => {
            console.error(`Error transcoding to ${resolution.name}:`, err);
            reject(err);
          })
          .run();
      });
    });
    
    await Promise.all(promises);
    console.log("All transcodings completed");
    process.exit(0);
  } catch (err) {
    console.error("Error checking object:", err);
  }
};

init().catch((error) => {
  console.error("Error initializing S3 Client:", error);
}).finally(() => {
  process.exit(0);
});