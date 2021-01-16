import MPI from "mpi-node";

export default async function handlePurchaser(initData) {
  let liftLocation = {};
  let queue = {};
  let liftCritical = {};
  let ackCounter = 0;
  let currentLiftDepartureTime = null;
  let ordersToSend = [];
  let waitingForLiftSharedStatus = false;
  let waitingForPackage = false;

  initState();
  handleLiftRequest();
  handleLiftAckRequest();
  handleReleaseOthersRequest();
  handleReleaseAllRequest();
  handleLiftSharedRequest();
  handleLiftSharedAnswerRequest();
  handleLiftSharedStatusRequest();
  handleOrdersSentRequest();
  handlePackagesSentRequest();

  await sleep(1000);

  broadcastLift();

  function initState() {
    for (let i = 1; i <= initData.liftsNumber; i++) {
      liftLocation[i] = [{ status: "UP_ORDERING", timestamp: getTimestamp() }];
      queue[i] = [];
      liftCritical[i] = null;
    }
  }

  function getLiftLocation(liftKey) {
    const sorted = liftLocation[liftKey].sort(
      (a, b) => b.timestamp - a.timestamp
    );
    return sorted[0].status;
  }

  function handleLiftRequest() {
    MPI.recv("LIFT", (msg) => {
      Object.keys(queue).forEach((key) => {
        queue[key].push({ tid: msg.tid, timestamp: msg.timestamp });
        queue[key] = queue[key].sort((a, b) => a.timestamp - b.timestamp);
      });
      MPI.send(msg.tid, { type: "LIFT_ACK" });
    });
  }

  function handleLiftAckRequest() {
    MPI.recv("LIFT_ACK", async (msg) => {
      ackCounter++;
      if (ackCounter == initData.purchasersSize) {
        ackCounter = 0;
        tryAccessLift();
      }
    });
  }

  function handleReleaseOthersRequest() {
    MPI.recv("LIFT_RELEASE_OTHERS", (msg) => {
      liftCritical[msg.busyKey] = msg.processTid;
      const keysToRelease = Object.keys(queue).filter(
        (key) => key != msg.busyKey
      );
      keysToRelease.forEach((key) => {
        queue[key] = queue[key].filter((el) => el.tid != msg.processTid);
      });
    });
  }

  function handleReleaseAllRequest() {
    MPI.recv("LIFT_RELEASE_ALL", (msg) => {
      Object.keys(queue).forEach((key) => {
        queue[key] = queue[key].filter((el) => el.tid != msg.processTid);
      });
    });
  }

  function handleLiftSharedRequest() {
    MPI.recv("LIFT_SHARED", (msg) => {
      waitingForLiftSharedStatus = true;
      MPI.send(msg.processTid, {
        type: "LIFT_SHARED_ANSWER",
        order: generateOrder(msg.liftKey),
        processTid: initData.tid,
      });
    });
  }

  function handleLiftSharedAnswerRequest() {
    MPI.recv("LIFT_SHARED_ANSWER", (msg) => {
      const now = new Date();
      if (currentLiftDepartureTime && currentLiftDepartureTime > now) {
        MPI.send(msg.processTid, {
          type: "LIFT_SHARED_OK",
          processTid: initData.tid,
        });
        ordersToSend.push(msg.order);
      } else {
        MPI.send(msg.processTid, {
          type: "LIFT_SHARED_GONE",
          processTid: initData.tid,
        });
      }
    });
  }

  function handleLiftSharedStatusRequest() {
    MPI.recv("LIFT_SHARED_OK", () => {
      waitingForLiftSharedStatus = false;
      waitingForPackage = true;
      MPI.broadcast({ type: "LIFT_RELEASE_ALL", processTid: initData.tid });
      broadcastLift();
    });
    MPI.recv("LIFT_SHARED_GONE", () => {
      waitingForLiftSharedStatus = false;
    });
  }

  function handleOrdersSentRequest() {
    MPI.recv("ORDERS_SENT", (msg) => {
      liftCritical[msg.liftKey] = null;
      queue[msg.liftKey] = queue[msg.liftKey].filter(
        (el) => el.tid != msg.processTid
      );
      liftLocation[msg.liftKey].push({
        status: "DOWN_GET",
        timestamp: getTimestamp(),
      });
    });
  }

  function handlePackagesSentRequest() {
    MPI.recv("PACKAGES_SENT", (msg) => {
      liftLocation[msg.packages[0].liftKey].push({
        status: "UP_ORDERING",
        timestamp: getTimestamp(),
      });
      const myPackage = msg.packages.find((el) => el.tid == initData.tid);
      if (myPackage) {
        MPI.broadcast({
          type: "Finally! Ive got my package",
          tid: initData.tid,
          myPackage,
        });
        waitingForPackage = false;
        broadcastLift();
      }
    });
  }

  function broadcastLift() {
    MPI.broadcast({
      type: "LIFT",
      tid: initData.tid,
      timestamp: getTimestamp(),
    });
  }

  async function tryAccessLift() {
    let key;
    while (!key && !waitingForPackage && !waitingForLiftSharedStatus) {
      key = getFreeLiftKey();
      await sleep(1000);
    }
    if (key) {
      console.log(`LIFT (sending order): ${key} - process ${initData.tid}`);
      MPI.broadcast({
        type: "LIFT_RELEASE_OTHERS",
        busyKey: key,
        processTid: initData.tid,
      });
      const order = generateOrder(key);
      ordersToSend.push(order);
      const departure = generateDepartureTime();
      currentLiftDepartureTime = departure.departureDate;
      queue[key].forEach((el) => {
        if (el.tid !== initData.tid)
          MPI.send(el.tid, {
            type: "LIFT_SHARED",
            liftKey: key,
            processTid: initData.tid,
          });
      });
      await sleep(departure.miliseconds);
      currentLiftDepartureTime = null;
      MPI.broadcast({
        type: "ORDERS_SENT",
        ordersToSend,
        liftKey: key,
        processTid: initData.tid,
      });
      ordersToSend = [];
      waitingForPackage = true;
    }
  }

  function generateOrder(liftKey) {
    return {
      tid: initData.tid,
      packagesNumber: generateRandom(1, initData.liftCapacity),
      liftKey,
      timestamp: getTimestamp(),
    };
  }

  function generateDepartureTime() {
    let date = new Date();
    const miliseconds = generateRandom(3, 6);
    date.setMilliseconds(date.getMilliseconds() + miliseconds);
    return { departureDate: date, miliseconds };
  }

  function generateRandom(min, max) {
    return Math.floor(Math.random() * (max - min) + min);
  }

  function getFreeLiftKey() {
    for (const key of Object.keys(liftCritical)) {
      if (
        liftCritical[key] === null &&
        getLiftLocation(key) === "UP_ORDERING" &&
        queue[key][0] &&
        queue[key][0].tid === initData.tid
      ) {
        return key;
      }
    }
    return null;
  }

  function getTimestamp() {
    const hrTime = process.hrtime();
    return hrTime[0] * 1000000 + hrTime[1] / 1000;
  }

  async function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}
