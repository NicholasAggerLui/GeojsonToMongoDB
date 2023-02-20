const mongoose = require("mongoose");
const fs = require("fs");
const { MongoMemoryServer } = require("mongodb-memory-server");
const JSONStream = require("JSONStream");
const { Schema } = mongoose;

const fn = async () => {
    const mongod = await MongoMemoryServer.create();
    const uri = mongod.getUri();

    await mongoose.connect(uri);
    await readGeoJSON();
    console.log(uri);
};

const landSchema = new Schema({
    FID: { type: Number },
    lat: { type: Number },
    lon: { type: Number },
    sequence: { type: Number },
    nextNode: { type: String },
});
const Land = mongoose.model("LandPolygon", landSchema);

const addFeature = async (fid, lon, lat, sequence) => {
    const land = new Land({
        FID: fid,
        lon: lon,
        lat: lat,
        sequence: sequence,
        nextNode: "",
    });

    try {
        const savedLand = await land.save();
        console.log(`Saved FID: ${fid}`);
        await updateNextNode(sequence, fid, savedLand);
        return savedLand;
    } catch (err) {
        console.error(err.message);
    }
};

async function readGeoJSON() {
    const filePath = "./sample.json";
    const stream = fs
        .createReadStream(filePath, { encoding: "utf8" })
        .pipe(JSONStream.parse("features.*"));

    stream.on("data", async function (feature) {
        const { properties, geometry } = feature;
        const { coordinates } = geometry;

        if (geometry.type === "MultiPolygon") {
            coordinates.forEach((polygon) => {
                polygon.forEach((coordSet) => {
                    coordSet.forEach(async (coords, index) => {
                        try {

                            const currentLand = await addFeature(properties.FID, coords[0], coords[1], index + 1);

                            console.log(`Saved: ${currentLand._id}`)
                        } catch (err) {
                            console.error(err.message);
                        }
                    });
                });
            });
        }
    });
}
async function updateNextNode(sequence, fid, currentLand){
    const prevLand = await Land.findOne({ FID: fid, sequence: sequence - 1 });
    if(prevLand){
        prevLand.nextNode = currentLand._id;
        await prevLand.save();
    }
}
fn().then(() => console.log("DONE!"));

