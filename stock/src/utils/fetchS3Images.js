import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import dotenv from 'dotenv';
dotenv.config();

const s3 = new S3Client({
  region: process.env.REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

export async function fetchS3Images() {
  try {
    const params = {
      Bucket: process.env.S3_BUCKET_NAME,
      Prefix: '', // Specify folder if necessary
    };

    // Fetch the list of objects in the bucket
    const command = new ListObjectsV2Command(params);
    const data = await s3.send(command);

    const images = data.Contents.map((file) => ({
      url: `https://${process.env.S3_BUCKET_NAME}.s3.${process.env.REGION}.amazonaws.com/${file.Key}`,
      key: file.Key,
      lastModified: file.LastModified,
    })).sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified)); // Sort by latest date

    return images;
  } catch (err) {
    console.error('Error fetching S3 images:', err);
    return [];
  }
}
