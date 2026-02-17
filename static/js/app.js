let businesses = [];
const PASSWORD = "changeme123";

function checkPassword() {
  const input = document.getElementById("password").value;
  if (input === PASSWORD) {
    document.getElementById("login-screen").style.display = "none";
    document.getElementById("app").style.display = "block";
    loadData();
  } else {
    document.getElementById("login-error").innerText = "Wrong password";
  }
}

async function loadData() {
  const res = await fetch("data/businesses.json");
  businesses = await res.json();
  populateStates();
  render(businesses);
}

function populateStates() {
  const states = [...new Set(businesses.map(b => b.state))].sort();
  const select = document.getElementById("stateFilter");
  states.forEach(state => {
    const opt = document.createElement("option");
    opt.value = state;
    opt.innerText = state;
    select.appendChild(opt);
  });
}

function render(list) {
  const container = document.getElementById("results");
  container.innerHTML = "";
  list.forEach(b => {
    container.innerHTML += `
      <div class="card">
        <h3>${b.name}</h3>
        <p>${b.city}, ${b.state}</p>
        <p>${b.category || ""}</p>
        <p>${b.phone || ""}</p>
      </div>
    `;
  });
}

document.getElementById("search").addEventListener("input", filter);
document.getElementById("stateFilter").addEventListener("change", filter);

function filter() {
  const text = document.getElementById("search").value.toLowerCase();
  const state = document.getElementById("stateFilter").value;
  const filtered = businesses.filter(b =>
    b.name.toLowerCase().includes(text) &&
    (state === "" || b.state === state)
  );
  render(filtered);
}

// Dark Mode
document.getElementById("toggleTheme").addEventListener("click", () => {
  document.body.classList.toggle("dark");
});
