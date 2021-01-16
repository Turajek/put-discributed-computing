import MPI from "mpi-node";

export default async function handleCourier(initData) {
  let liftLocation = {};
  let queueGetPackage = {};
  let queueSendPackage = {};
  let orders = {};
  let packagesToSend = [];
  let liftCritical = {};
  let preparedPackages = [];
  let ackGetPackageCounter = 0;
  let ackSendPackageCounter = 0;
  let currentLiftDepartureTime;
  let currentCapacity = 0;
  let waitingPackage = null;

  initState();
  handleOrdersSentRequest();
  handleLiftGetPackageRequest();
  handleLiftSendPackageRequest();
  handleLiftGetPackageAckRequest();
  handleLiftGetReleaseRequest();
  handleLiftSendPackageAckRequest();
  handleLiftDownPostRequest();
  handleLiftSharedRequest();
  handleLiftSharedAnswerRequest();
  handleLiftSharedStatusRequest();
  handleLiftSendReleaseRequest();

  await sleep(1000);

  broadcastGetPackage();

  function initState() {
    for (let i = 1; i <= initData.liftsNumber; i++) {
      liftLocation[i] = [{ status: "UP_ORDERING", timestamp: getTimestamp() }];
      queueGetPackage[i] = [];
      queueSendPackage[i] = [];
      orders[i] = [];
      liftCritical[i] = null;
    }
  }

  function getLiftLocation(liftKey) {
    const sorted = liftLocation[liftKey].sort(
      (a, b) => b.timestamp - a.timestamp
    );
    return sorted[0].status;
  }

  function handleOrdersSentRequest() {
    MPI.recv("ORDERS_SENT", (msg) => {
      liftLocation[msg.liftKey].push({
        status: "DOWN_GET",
        timestamp: getTimestamp(),
      });

      orders[msg.liftKey] = [...orders[msg.liftKey], ...msg.ordersToSend];
      orders[msg.liftKey] = orders[msg.liftKey].sort(
        (a, b) => a.timestamp - b.timestamp
      );
    });
  }

  function handleLiftGetPackageRequest() {
    MPI.recv("LIFT_GET_PACKAGE", (msg) => {
      Object.keys(queueGetPackage).forEach((key) => {
        queueGetPackage[key].push({ tid: msg.tid, timestamp: msg.timestamp });
        queueGetPackage[key] = queueGetPackage[key].sort(
          (a, b) => a.timestamp - b.timestamp
        );
      });
      MPI.send(msg.tid, { type: "LIFT_GET_PACKAGE_ACK" });
    });
  }

  function handleLiftSendPackageRequest() {
    MPI.recv("LIFT_SEND_PACKAGE", (msg) => {
      msg.liftKeys.forEach((key) => {
        queueSendPackage[key].push({
          tid: msg.tid,
          timestamp: msg.timestamp,
        });
        queueSendPackage[key] = queueSendPackage[key].sort(
          (a, b) => a.timestamp - b.timestamp
        );
      });

      MPI.send(msg.tid, { type: "LIFT_SEND_PACKAGE_ACK" });
    });
  }

  function handleLiftGetPackageAckRequest() {
    MPI.recv("LIFT_GET_PACKAGE_ACK", async () => {
      ackGetPackageCounter++;
      if (ackGetPackageCounter == initData.couriersSize) {
        ackGetPackageCounter = 0;
        await tryGetPackage();
      }
    });
  }
  function handleLiftSendPackageAckRequest() {
    MPI.recv("LIFT_SEND_PACKAGE_ACK", async () => {
      ackSendPackageCounter++;
      if (ackSendPackageCounter == initData.couriersSize) {
        ackSendPackageCounter = 0;
        await trySendPackage();
      }
    });
  }

  function handleLiftGetReleaseRequest() {
    MPI.recv("LIFT_GET_RELEASE", (msg) => {
      liftCritical[msg.liftKey] = null;
      Object.keys(queueGetPackage).forEach((key) => {
        queueGetPackage[key].shift();
      });
      queueGetPackage[msg.liftKey].shift();
      orders[msg.liftKey].shift();
      console.log(orders);
    });
  }

  function handleLiftDownPostRequest() {
    MPI.recv("LIFT_DOWN_POST", (msg) => {
      liftLocation[msg.liftKey].push({
        status: "DOWN_POST",
        timestamp: getTimestamp(),
      });
    });
  }

  function handleLiftSharedRequest() {
    MPI.recv("LIFT_SHARED_DOWN", (msg) => {
      waitingPackage = preparedPackages.find(
        (el) => el.liftKey === msg.liftKey
      );
      waitingPackage.index = preparedPackages.findIndex(
        (el) => el.liftKey === msg.liftKey
      );
      MPI.send(msg.processTid, {
        type: "LIFT_SHARED_DOWN_ANSWER",
        package: waitingPackage,
        processTid: initData.tid,
      });
    });
  }

  function handleLiftSharedAnswerRequest() {
    MPI.recv("LIFT_SHARED_DOWN_ANSWER", (msg) => {
      const now = new Date();
      if (
        currentLiftDepartureTime &&
        currentLiftDepartureTime > now &&
        currentCapacity + msg.package.packagesNumber <= initData.liftCapacity
      ) {
        MPI.send(msg.processTid, {
          type: "LIFT_SHARED_DOWN_OK",
          processTid: initData.tid,
        });
        packagesToSend.push(msg.package);
        currentCapacity += msg.package.packagesNumber;
      } else {
        MPI.send(msg.processTid, {
          type: "LIFT_SHARED_DOWN_GONE",
          processTid: initData.tid,
        });
      }
    });
  }

  function handleLiftSharedStatusRequest() {
    MPI.recv("LIFT_SHARED_DOWN_OK", () => {
      preparedPackages.splice(waitingPackage.index, 1);
      broadcastLiftSendRelease(waitingPackage.liftKey);
      waitingPackage = null;
      if (preparedPackages.length) {
        broadcastLiftSendPackage();
      } else {
        broadcastGetPackage();
      }
    });
    MPI.recv("LIFT_SHARED_DOWN_GONE", () => {
      waitingPackage = null;
    });
  }

  function handleLiftSendReleaseRequest() {
    MPI.recv("LIFT_SEND_RELEASE", (msg) => {
      liftCritical[msg.liftKey] = null;
      queueSendPackage[msg.liftKey].shift();
    });
  }

  function broadcastGetPackage() {
    MPI.broadcast({
      type: "LIFT_GET_PACKAGE",
      tid: initData.tid,
      timestamp: getTimestamp(),
    });
  }

  function broadcastLiftSendRelease(liftKey) {
    MPI.broadcast({
      type: "LIFT_SEND_RELEASE",
      liftKey,
    });
  }

  function broadcastLiftSendPackage() {
    MPI.broadcast({
      type: "LIFT_SEND_PACKAGE",
      tid: initData.tid,
      liftKeys: preparedPackages.map((el) => el.liftKey),
      timestamp: getTimestamp(),
    });
  }

  function getFreeLiftGetKey() {
    for (const key of Object.keys(liftCritical)) {
      if (
        liftCritical[key] === null &&
        getLiftLocation(key) === "DOWN_GET" &&
        queueGetPackage[key][0] &&
        queueGetPackage[key][0].tid === initData.tid &&
        orders[key].length
      ) {
        return key;
      }
    }
    return null;
  }
  function getFreeLiftSendKey() {
    for (const key of Object.keys(liftCritical)) {
      if (
        liftCritical[key] === null &&
        getLiftLocation(key) === "DOWN_POST" &&
        queueSendPackage[key][0] &&
        queueSendPackage[key][0].tid === initData.tid &&
        preparedPackages.some((el) => el.liftKey === key)
      ) {
        return key;
      }
    }
    return null;
  }

  function getCanResign() {
    return (
      preparedPackages.length &&
      Object.keys(orders).every((key) => !orders[key].length)
    );
  }

  async function tryGetPackage() {
    let key;
    while (!key) {
      key = getFreeLiftGetKey();
      await sleep(500);
      if (getCanResign()) {
        broadcastLiftSendPackage(preparedPackages.map((el) => el.liftKey));
        break;
      }
    }
    if (key) {
      console.log(`LIFT (getting order): ${key} - process ${initData.tid}`);
      const orderToPrepare = { ...orders[key][0] };
      if (orders[key].length === 1) {
        MPI.broadcast({ type: "LIFT_DOWN_POST", liftKey: key });
      }
      MPI.broadcast({ type: "LIFT_GET_RELEASE", liftKey: key });

      await prepareOrder(orderToPrepare);
      if (allOrdersDelegated()) {
        broadcastLiftSendPackage();
      } else {
        broadcastGetPackage();
      }
    }
  }

  async function prepareOrder(order) {
    await sleep(500);
    preparedPackages.push(order);
  }

  function allOrdersDelegated() {
    for (const key of Object.keys(orders)) {
      if (orders[key].length) {
        return false;
      }
    }
    return true;
  }
  async function trySendPackage() {
    let key;
    while (!key) {
      key = getFreeLiftSendKey();
      await sleep(500);
    }
    if (key) {
      console.log(`LIFT (sending package): ${key} - process ${initData.tid}`);

      preparedPackages.forEach((packageItem) => {
        if (
          currentCapacity + packageItem.packagesNumber <=
          initData.liftCapacity
        ) {
          packagesToSend.push(packageItem);
          packageItem.toRemove = true;
          currentCapacity += packageItem.packagesNumber;
        }
      });
      preparedPackages = preparedPackages.filter((el) => !el.toRemove);

      if (currentCapacity < initData.liftCapacity) {
        const departure = generateDepartureTime();
        currentLiftDepartureTime = departure.departureDate;
        queueSendPackage[key].forEach((el) => {
          if (el.tid !== initData.tid) {
            MPI.send(el.tid, {
              type: "LIFT_SHARED_DOWN",
              liftKey: key,
              processTid: initData.tid,
            });
          }
        });
        await sleep(departure.miliseconds);
        currentLiftDepartureTime = null;
      }

      broadcastLiftSendRelease(key);

      MPI.broadcast({
        type: "PACKAGES_SENT",
        packages: packagesToSend,
      });
      packagesToSend = [];
      currentCapacity = 0;
      if (preparedPackages.length) {
        broadcastLiftSendPackage();
      } else {
        broadcastGetPackage();
      }
    }
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
  function generateDepartureTime() {
    let date = new Date();
    const miliseconds = generateRandom(3, 6);
    date.setMilliseconds(date.getMilliseconds() + miliseconds);
    return { departureDate: date, miliseconds };
  }

  function generateRandom(min, max) {
    return Math.floor(Math.random() * (max - min) + min);
  }
}
