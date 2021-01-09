import MPI from "mpi-node";

export default async function handleCourier(initData) {
  let liftLocation = {};
  let queueGetPackage = {};
  let queueSendPackage = {};
  let orders = {};
  let packagesToSend = {};
  let liftCritical = {};
  let preparedOrders = [];
  let ackGetPackageCounter = 0;
  let ackSendPackageCounter = 0;

  initState();
  handleOrdersSentRequest();
  handleLiftGetPackageRequest();
  handleLiftSendPackageRequest();
  handleLiftGetPackageAckRequest();
  handleLiftGetReleaseRequest();
  handleLiftSendPackageAckRequest();
  handleLiftDownPostRequest();
  await sleep(1000);

  broadcastGetPackage();

  function initState() {
    for (let i = 1; i <= initData.liftsNumber; i++) {
      liftLocation[i] = "UP_ORDERING";
      queueGetPackage[i] = [];
      queueSendPackage[i] = [];
      orders[i] = [];
      liftCritical[i] = null;
      packagesToSend[i] = [];
    }
  }

  function handleOrdersSentRequest() {
    MPI.recv("ORDERS_SENT", (msg) => {
      liftLocation[msg.liftKey] = "DOWN_GET";
      orders[msg.liftKey] = [...orders[msg.liftKey], ...msg.ordersToSend];
      orders[msg.liftKey] = orders[msg.liftKey].sort(
        (a, b) => a.timestamp - b.timestamp
      );
      console.log(orders);
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
      console.log(queueGetPackage);
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
      console.log(queueSendPackage);
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
        trySendPackage();
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
    });
  }

  function handleLiftDownPostRequest() {
    MPI.recv("LIFT_DOWN_POST", (msg) => {
      liftLocation[msg.liftKey] = "LIFT_DOWN_POST";
    });
  }

  function broadcastGetPackage() {
    MPI.broadcast({
      type: "LIFT_GET_PACKAGE",
      tid: initData.tid,
      timestamp: getTimestamp(),
    });
  }

  function getFreeLiftGetKey() {
    for (const key of Object.keys(liftCritical)) {
      if (
        liftCritical[key] === null &&
        liftLocation[key] === "DOWN_GET" &&
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
        liftLocation[key] === "DOWN_GET" &&
        queueGetPackage[key][0] &&
        queueGetPackage[key][0].tid === initData.tid &&
        orders[key].length
      ) {
        return key;
      }
    }
    return null;
  }

  function getCanResign() {
    return (
      preparedOrders.length &&
      Object.keys(orders).every((key) => !orders[key].length)
    );
  }

  async function tryGetPackage() {
    let key;
    while (!key) {
      key = getFreeLiftGetKey();
      await sleep(500);
      if (getCanResign()) {
        MPI.broadcast({
          type: "LIFT_SEND_PACKAGE",
          tid: initData.tid,
          liftKeys: preparedOrders.map((el) => el.liftKey),
          timestamp: getTimestamp(),
        });
        break;
      }
    }
    if (key) {
      console.log(orders);
      console.log(`LIFT: ${key} - process ${initData.tid}`);
      const orderToPrepare = { ...orders[key][0] };
      if (orders[key].length === 1) {
        MPI.broadcast({ type: "LIFT_DOWN_POST", liftKey: key });
      }
      MPI.broadcast({ type: "LIFT_GET_RELEASE", liftKey: key });

      await prepareOrder(orderToPrepare);
      if (allOrdersDelegated()) {
        MPI.broadcast({
          type: "LIFT_SEND_PACKAGE",
          tid: initData.tid,
          liftKeys: preparedOrders.map((el) => el.liftKey),
          timestamp: getTimestamp(),
        });
      } else {
        broadcastGetPackage();
      }
    }
    //
  }

  async function prepareOrder(order) {
    console.log("preparing order:");
    console.log(order);
    await sleep(500);
    preparedOrders.push(order);
  }

  function allOrdersDelegated() {
    for (const key of Object.keys(orders)) {
      if (orders[key].length) {
        return false;
      }
    }
    return true;
  }
  function trySendPackage() {
    //
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
