const sharp = require('sharp');
const path = require('path');
const fs = require('fs');

class WatermarkProcessor {
  constructor() {
    this.watermarkPath = path.join(__dirname, '../public/images/watermark.png');
    this.defaultWatermarkText = 'Codentra ©';
  }

  // Add watermark to a single image
  async addWatermark(imagePath, options = {}) {
    try {
      const {
        position = 'bottom-right',
        opacity = 0.7,
        scale = 0.15,
        margin = 20
      } = options;

      // Check if watermark file exists
      let watermarkBuffer;
      if (fs.existsSync(this.watermarkPath)) {
        watermarkBuffer = fs.readFileSync(this.watermarkPath);
      } else {
        // Create text watermark if image doesn't exist
        watermarkBuffer = await this.createTextWatermark();
      }

      // Get image dimensions
      const imageInfo = await sharp(imagePath).metadata();
      const { width, height } = imageInfo;

      // Calculate watermark size
      const watermarkSize = Math.min(width, height) * scale;
      
      // Process watermark
      const processedWatermark = await sharp(watermarkBuffer)
        .resize(Math.round(watermarkSize), Math.round(watermarkSize), {
          fit: 'inside',
          withoutEnlargement: true
        })
        .composite([{
          input: Buffer.from([255, 255, 255, 255]), // White background for opacity
          raw: { width: 1, height: 1, channels: 4 }
        }])
        .modulate({
          brightness: 1,
          saturation: 0.8
        })
        .png()
        .toBuffer();

      // Calculate position
      const availablePositions = {
        'top-left': { left: margin, top: margin },
        'top-right': { left: width - watermarkSize - margin, top: margin },
        'bottom-left': { left: margin, top: height - watermarkSize - margin },
        'bottom-right': { left: width - watermarkSize - margin, top: height - watermarkSize },
        'center': { left: (width - watermarkSize) / 2, top: (height - watermarkSize) / 2 }
      };

      const finalPosition = availablePositions[options.position] || availablePositions['bottom-right'];

      // Apply watermark
      const result = await sharp(imagePath)
        .composite([{
          input: processedWatermark,
          left: Math.round(finalPosition.left),
          top: Math.round(finalPosition.top),
          blend: 'over'
        }])
        .png()
        .toBuffer();

      return result;
    } catch (error) {
      console.error('Error adding watermark:', error);
      throw error;
    }
  }

  // Create text watermark
  async createTextWatermark() {
    const svg = `
      <svg width="200" height="60" xmlns="http://www.w3.org/2000/svg">
        <rect width="100%" height="100%" fill="rgba(255,255,255,0.1)" rx="5"/>
        <text x="50%" y="50%" font-family="Arial, sans-serif" font-size="18" 
              font-weight="bold" text-anchor="middle" dominant-baseline="middle" 
              fill="rgba(255,255,255,0.8)">
          ${this.defaultWatermarkText}
        </text>
      </svg>
    `;

    return await sharp(Buffer.from(svg))
      .png()
      .toBuffer();
  }

  // Process multiple images
  async processImages(imagePaths, outputPath, options = {}) {
    const results = [];
    
    for (const imagePath of imagePaths) {
      try {
        const watermarkedBuffer = await this.addWatermark(imagePath, options);
        const filename = path.basename(imagePath);
        const outputFilePath = path.join(outputPath, `watermarked-${filename}`);
        
        await sharp(watermarkedBuffer).toFile(outputFilePath);
        results.push({
          original: imagePath,
          watermarked: outputFilePath,
          success: true
        });
      } catch (error) {
        console.error(`Failed to process ${imagePath}:`, error);
        results.push({
          original: imagePath,
          watermarked: null,
          success: false,
          error: error.message
        });
      }
    }
    
    return results;
  }

  // Add watermark to image buffer (for upload processing)
  async addWatermarkToBuffer(imageBuffer, options = {}) {
    try {
      const {
        position = 'bottom-right',
        opacity = 0.7,
        scale = 0.15,
        margin = 20
      } = options;

      // Get image info from buffer
      const imageInfo = await sharp(imageBuffer).metadata();
      const { width, height } = imageInfo;

      // Get or create watermark
      let watermarkBuffer;
      if (fs.existsSync(this.watermarkPath)) {
        watermarkBuffer = fs.readFileSync(this.watermarkPath);
      } else {
        watermarkBuffer = await this.createTextWatermark();
      }

      // Calculate watermark size
      const watermarkSize = Math.min(width, height) * scale;

      // Process watermark
      const processedWatermark = await sharp(watermarkBuffer)
        .resize(Math.round(watermarkSize), Math.round(watermarkSize), {
          fit: 'inside',
          withoutEnlargement: true
        })
        .png()
        .toBuffer();

      // Calculate position
      const availablePositions = {
        'top-left': { left: margin, top: margin },
        'top-right': { left: width - watermarkSize - margin, top: margin },
        'bottom-left': { left: margin, top: height - watermarkSize - margin },
        'bottom-right': { left: width - watermarkSize - margin, top: height - watermarkSize },
        'center': { left: (width - watermarkSize) / 2, top: (height - watermarkSize) / 2 }
      };

      const finalPos = availablePositions[position] || availablePositions['bottom-right'];

      // Apply watermark
      return await sharp(imageBuffer)
        .composite([{
          input: processedWatermark,
          left: Math.round(finalPos.left),
          top: Math.round(finalPos.top),
          blend: 'over'
        }])
        .png()
        .toBuffer();
    } catch (error) {
      console.error('Error adding watermark to buffer:', error);
      throw error;
    }
  }
}

module.exports = WatermarkProcessor;
