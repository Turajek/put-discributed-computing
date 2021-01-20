import MPI from "mpi-node";
import handleCourier from "./handleCourier.mjs";
import handlePurchaser from "./handlePurchaser.mjs";

const liftsNumber = 4;
const liftCapacity = 5;

MPI.init(main);

async function main() {
  const tid = MPI.rank();
  const size = MPI.size();
  let purchasersSize;
  let completedCounter = 0;

  MPI.recv("ROLE", (msg) => {
    const role = msg.content.purchasers.includes(tid) ? "purchaser" : "courier";
    const initData = {
      tid,
      size,
      liftsNumber,
      liftCapacity,
      purchasersSize: msg.content.purchasers.length,
      couriersSize: msg.content.couriers.length,
    };
    purchasersSize = msg.content.purchasers.length;
    if (role === "purchaser") {
      handlePurchaser(initData);
    } else {
      handleCourier(initData);
    }
  });

  MPI.recv("COMPLETED", () => {
    completedCounter++;
    console.log("completedCounter: " + completedCounter);
    if (completedCounter === purchasersSize) {
      console.log("The alghoritm was successfull");
      process.exit();
    }
  });

  if (tid === 0) {
    await sleep(1000);
    const roles = getRoles(size);
    MPI.broadcast({
      type: "ROLE",
      content: roles,
    });
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function getRoles(size) {
  let purchasers = [];
  let couriers = [];
  for (let i = 0; i < size; i++) {
    const randomBoolean = Math.random() < 0.5;
    if (randomBoolean) {
      purchasers.push(i);
    } else {
      couriers.push(i);
    }
  }
  if (purchasers.length === couriers.length) {
    const removedLastPurchaser = purchasers.pop();
    couriers.push(removedLastPurchaser);
  } else if (purchasers.length === 0) {
    const removedLastCourier = couriers.pop();
    purchasers.push(removedLastCourier);
  } else if (couriers.length === 0) {
    const removedLastPurchaser = purchasers.pop();
    couriers.push(removedLastPurchaser);
  }
  return { purchasers, couriers };
}
