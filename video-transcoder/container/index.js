const { S3Client, HeadObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const ffmpeg = require("fluent-ffmpeg");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");

const AWS_REGION = "ap-south-1";
const AWS_BUCKET_NAME = "sehncoded";
const s3Client = new S3Client({ region: AWS_REGION });
const key = "uploads/1758779722327-v_6cf31daa-7eb0-4a9b-aa07-78be28c59d37.mp4";

const resolutions = [
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
      await fsp.writeFile(originalPath, response.Body);
      
      resolutions.forEach(resolution => {
        const outputPath = `videos/${resolution.name}-video.mp4`;
        ffmpeg(originalPath)
          .output(outputPath)
          .withVideoCodec('libx264')
          .withAudioCodec('aac')
          .withSize(`${resolution.width}x${resolution.height}`)
          .on('end', async () => {
            console.log(`Transcoding to ${resolution.name} completed`);
          })
          .on('error', (err) => {
            console.error(`Error transcoding to ${resolution.name}:`, err);
          })
          .save(outputPath);
      });
      //await fs.writeFile(path.join('/tmp', 'original-video.mp4'), response.Body);
    } catch (err) {
      console.error("Error checking object:", err);
    }
};

init().catch((error) => {
  console.error("Error initializing S3 Client:", error);
});