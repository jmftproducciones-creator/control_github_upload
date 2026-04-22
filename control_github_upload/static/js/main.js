function yesNoSelect(name) {
    return `
        <select name="${name}" class="form-select form-select-sm">
            <option value="si">Sí</option>
            <option value="no" selected>No</option>
        </select>
    `;
}

function removeRow(btn) {
    const tr = btn.closest('tr');
    if (tr) tr.remove();
}

function addPersonalRow() {
    const tbody = document.querySelector('#tablaPersonal tbody');
    if (!tbody) return;

    const tr = document.createElement('tr');
    tr.innerHTML = `
        <td><input type="text" name="personal_nombre[]" class="form-control form-control-sm"></td>
        <td>${yesNoSelect('personal_conoce[]')}</td>
        <td>${yesNoSelect('personal_capacitacion[]')}</td>
        <td>${yesNoSelect('personal_requiere[]')}</td>
        <td><input type="text" name="personal_observacion[]" class="form-control form-control-sm"></td>
        <td><button type="button" class="btn btn-sm btn-outline-danger" onclick="removeRow(this)">X</button></td>
    `;
    tbody.appendChild(tr);
}
function addDocumentoRow() {
    const tbody = document.querySelector('#tablaDocumentos tbody');
    if (!tbody) return;

    const tr = document.createElement('tr');
    tr.innerHTML = `
        <td><input type="text" name="documento_codigo[]" class="form-control form-control-sm"></td>
        <td><input type="text" name="documento_revision[]" class="form-control form-control-sm"></td>
        <td>
            <select name="documento_estado[]" class="form-select form-select-sm">
                <option value="correcto">Correcto</option>
                <option value="documento_obsoleto">Documento obsoleto</option>
                <option value="sin_copia_controlada">Sin copia controlada</option>
                <option value="documento_no_controlado">Documento no controlado</option>
                <option value="otro">Otro</option>
            </select>
        </td>
        <td>
            <div class="input-group input-group-sm">
                <input type="text" name="documento_observacion[]" class="form-control" placeholder="Observación">
                <label class="btn btn-outline-secondary mb-0" title="Adjuntar Foto">
                    <i class="bi bi-camera"></i>
                    <input type="file" name="documento_foto[]" accept="image/*" style="display: none;" onchange="this.parentElement.classList.replace('btn-outline-secondary', 'btn-success')">
                </label>
            </div>
        </td>
        <td>
            <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeRow(this)">X</button>
        </td>
    `;
    tbody.appendChild(tr);
}

function addQuimicoRow() {
    const tbody = document.querySelector('#tablaQuimicos tbody');
    if (!tbody) return;

    const tr = document.createElement('tr');
    tr.innerHTML = `
        <td><input type="text" name="quimico_nombre[]" class="form-control form-control-sm"></td>
        <td>${yesNoSelect('quimico_llave[]')}</td>
        <td>${yesNoSelect('quimico_envase[]')}</td>
        <td>${yesNoSelect('quimico_etiqueta[]')}</td>
        <td>${yesNoSelect('quimico_hoja[]')}</td>
        <td><input type="text" name="quimico_observacion[]" class="form-control form-control-sm"></td>
        <td><input type="text" name="quimico_medida[]" class="form-control form-control-sm"></td>
        <td><button type="button" class="btn btn-sm btn-outline-danger" onclick="removeRow(this)">X</button></td>
    `;
    tbody.appendChild(tr);
}

// Lógica de Persistencia Global del Modo Oscuro/Claro
function toggleTheme() {
    let currentTheme = document.documentElement.getAttribute('data-theme');
    let newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    
    document.documentElement.setAttribute('data-theme', newTheme);
    document.documentElement.setAttribute('data-bs-theme', newTheme);
    localStorage.setItem('suite_theme', newTheme);
    
    // Disparar un evento para modulos de terceros si los hubiere
    window.dispatchEvent(new CustomEvent('themeChanged', { detail: { theme: newTheme } }));
}
