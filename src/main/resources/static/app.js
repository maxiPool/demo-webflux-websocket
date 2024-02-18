const stompClient = new StompJs.Client({
    brokerURL: 'ws://localhost:8080/handshake'
});

stompClient.onConnect = (frame) => {
    setConnected(true);
    console.log('Connected: ' + frame);
    stompClient.subscribe('/topic/greetings', (greeting) => {
        showGreeting(JSON.parse(greeting.body).content);
    });
    stompClient.subscribe('/topic/allRecords', (allRecords) => {
        showAllRecords(JSON.parse(allRecords.body));
    });
};

stompClient.onWebSocketError = (error) => {
    console.error('Error with websocket', error);
};

stompClient.onStompError = (frame) => {
    console.error('Broker reported error: ' + frame.headers['message']);
    console.error('Additional details: ' + frame.body);
};

function setConnected(connected) {
    $("#connect").prop("disabled", connected);
    $("#disconnect").prop("disabled", !connected);
    if (connected) {
        $("#conversation").show();
    } else {
        $("#conversation").hide();
    }
    $("#greetings").html("");
    $("#all-records").html("");
}

function connect() {
    stompClient.activate();
}

function disconnect() {
    stompClient.deactivate();
    setConnected(false);
    console.log("Disconnected");
}

function sendName() {
    stompClient.publish({
        destination: "/app/hello",
        body: JSON.stringify({'name': $("#name").val()})
    });
}

function sendTopic() {
    const sourceIds = [0, 1, 2, 3, 4];
    const topicId = $("#topic").val();
    console.log("Sending SUBSCRIBE to " + topicId);

    sourceIds.forEach(sourceId => {
        stompClient.subscribe(`/topic/${sourceId}/${topicId}`, (greeting) => {
            console.log("topic", greeting);
            const message = JSON.parse(greeting.body);
            console.log("received message ", message);
            showGreeting(JSON.stringify(message));
        });
    });
}

function unsubTopic() {
    const sourceIds = [0];
    const topicId = $("#topic").val();
    console.log("Sending UNSUBSCRIBE to " + topicId);

    sourceIds.forEach(sourceId => {
        stompClient.unsubscribe(`/topic/${sourceId}/${topicId}`, (greeting) => {
            console.log("topic", greeting);
            const message = JSON.parse(greeting.body);
            console.log("received message ", message);
            showGreeting(`unsubscribed ${sourceId}/${topicId}`);
        });
    });
}

function sendUpdatedRecord() {
    const field1 = $("#record-field1").val();
    const field2 = $("#record-field2").val();
    const id = $("#record-id").val();
    if (!!id) {
        stompClient.publish({
            destination: "/app/updateRecord",
            body: JSON.stringify({
                'id': id,
                'field1': !field1 ? null : field1,
                'field2': !field2 ? null : field2,
            })
        });
    }
}

function getAllRecords() {
    stompClient.publish({destination: "/app/getAllRecords"});
}

function showGreeting(message) {
    $("#greetings").append("<tr><td>" + message + "</td></tr>");
}

function showAllRecords(allRecords) {
    $("#all-records > tbody").html(allRecords?.map((r) => `<tr><td>${JSON.stringify(r)}</td></tr>`).join(''));
}

$(function () {
    $("form").on('submit', (e) => e.preventDefault());
    $("#connect").click(() => connect());
    $("#disconnect").click(() => disconnect());
    $("#send").click(() => sendName());
    $("#send-topic").click(() => sendTopic());
    $("#unsubscribe-topic").click(() => sendTopic());
    $("#update-record").click(() => sendUpdatedRecord());
    $("#get-all-records").click(() => getAllRecords());
});
