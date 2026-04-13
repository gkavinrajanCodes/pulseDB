// dashboard/index.js

const API_KEY = "pulse-db-secret-key";
const BASE_URL = "http://localhost:8000";
const WS_URL = "ws://localhost:8000/ws/subscribe/activity";

const activityFeed = document.getElementById('activity-feed');
const totalOpsEl = document.getElementById('total-ops');
const consoleInput = document.getElementById('console-input');
const consoleOutput = document.getElementById('console-output');
const sendBtn = document.getElementById('send-btn');

let totalOps = 0;

function addActivity(msg) {
    const item = document.createElement('div');
    item.className = 'activity-item';
    const now = new Date().toLocaleTimeString();
    item.innerHTML = `
        <span class="cmd">${msg}</span>
        <span class="time">${now}</span>
    `;
    activityFeed.prepend(item);
    if (activityFeed.children.length > 100) {
        activityFeed.lastChild.remove();
    }
    totalOps++;
    totalOpsEl.innerText = totalOps;
}

async function runCommand() {
    const raw = consoleInput.value.trim();
    if (!raw) return;
    
    const parts = raw.split(' ');
    const command = parts[0];
    const args = parts.slice(1);
    
    consoleOutput.innerText = "Processing...";
    
    try {
        const resp = await fetch(`${BASE_URL}/command`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-Key': API_KEY
            },
            body: JSON.stringify({ command, args })
        });
        const data = await resp.json();
        consoleOutput.innerText = `> ${JSON.stringify(data.result)}`;
        // Manually add to feed for visual feedback
        addActivity(`${command} ${args.join(' ')}`);
    } catch (e) {
        consoleOutput.innerText = `Error: ${e.message}`;
    }
    
    consoleInput.value = "";
}

sendBtn.addEventListener('click', runCommand);
consoleInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') runCommand();
});

// Setup WebSocket for real-time pub/sub activity
const socket = new WebSocket(WS_URL);

socket.onmessage = (event) => {
    addActivity(event.data);
};

socket.onopen = () => {
    console.log("Connected to PulseDB Activity Stream");
};

socket.onclose = () => {
    console.log("Disconnected from PulseDB Activity Stream");
};
