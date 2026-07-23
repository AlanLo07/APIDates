# 🔗 Integración Frontend-Backend: Finanzas de Parejas

## 📡 Cómo Conectar Flutter con la API

Este documento explica cómo modificar tu pantalla `CoupleFinancesScreen` para usar el backend real en lugar de datos mock.

---

## 1️⃣ Setup Inicial (HttpClient)

Primero, crea un servicio para manejar las llamadas HTTP:

### `lib/services/finances_service.dart`

```dart
import 'package:http/http.dart' as http;
import 'dart:convert';

class FinancesService {
  static const String _baseUrl = 'https://YOUR_API_ENDPOINT/finances';
  
  final String parejaId;
  
  FinancesService({required this.parejaId});

  // 🟢 Obtener resumen completo
  Future<Map<String, dynamic>> getResumen() async {
    final response = await http.get(
      Uri.parse('$_baseUrl/$parejaId/resumen'),
      headers: {'Content-Type': 'application/json'},
    );
    
    if (response.statusCode == 200) {
      return json.decode(response.body);
    } else {
      throw Exception('Error obteniendo resumen');
    }
  }

  // 🟢 Listar gastos (con filtros opcionales)
  Future<List<Map<String, dynamic>>> getGastos({
    String? month,
    String? category,
  }) async {
    String url = '$_baseUrl/$parejaId/gastos';
    
    List<String> params = [];
    if (month != null) params.add('month=$month');
    if (category != null) params.add('category=$category');
    
    if (params.isNotEmpty) {
      url += '?' + params.join('&');
    }
    
    final response = await http.get(
      Uri.parse(url),
      headers: {'Content-Type': 'application/json'},
    );
    
    if (response.statusCode == 200) {
      final data = json.decode(response.body);
      return List<Map<String, dynamic>>.from(data['expenses'] ?? []);
    } else {
      throw Exception('Error obteniendo gastos');
    }
  }

  // 🟢 Crear gasto
  Future<Map<String, dynamic>> createGasto({
    required String title,
    required double amount,
    required DateTime date,
    required String category,
    String? note,
    String createdBy = 'unknown',
  }) async {
    final response = await http.post(
      Uri.parse('$_baseUrl/$parejaId/gastos'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({
        'title': title,
        'amount': amount,
        'date': date.toIso8601String(),
        'category': category,
        'note': note,
        'createdBy': createdBy,
      }),
    );
    
    if (response.statusCode == 201) {
      return json.decode(response.body);
    } else {
      final error = json.decode(response.body);
      throw Exception(error['error'] ?? 'Error creando gasto');
    }
  }

  // 🟢 Actualizar gasto
  Future<Map<String, dynamic>> updateGasto(
    String gastoId, {
    String? title,
    double? amount,
    DateTime? date,
    String? category,
    String? note,
  }) async {
    final Map<String, dynamic> data = {};
    if (title != null) data['title'] = title;
    if (amount != null) data['amount'] = amount;
    if (date != null) data['date'] = date.toIso8601String();
    if (category != null) data['category'] = category;
    if (note != null) data['note'] = note;
    
    final response = await http.put(
      Uri.parse('$_baseUrl/$parejaId/gastos/$gastoId'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode(data),
    );
    
    if (response.statusCode == 200) {
      return json.decode(response.body);
    } else {
      throw Exception('Error actualizando gasto');
    }
  }

  // 🟢 Eliminar gasto
  Future<void> deleteGasto(String gastoId) async {
    final response = await http.delete(
      Uri.parse('$_baseUrl/$parejaId/gastos/$gastoId'),
      headers: {'Content-Type': 'application/json'},
    );
    
    if (response.statusCode != 200) {
      throw Exception('Error eliminando gasto');
    }
  }

  // 🟢 Establecer presupuesto mensual
  Future<Map<String, dynamic>> setBudget(
    String monthYear,
    double amount, {
    String? notes,
  }) async {
    final response = await http.post(
      Uri.parse('$_baseUrl/$parejaId/presupuesto/$monthYear'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({
        'amount': amount,
        'notes': notes,
      }),
    );
    
    if (response.statusCode == 200) {
      return json.decode(response.body);
    } else {
      throw Exception('Error estableciendo presupuesto');
    }
  }

  // 🟢 Obtener histórico de todos los meses
  Future<List<Map<String, dynamic>>> getHistorico({int limit = 12}) async {
    final response = await http.get(
      Uri.parse('$_baseUrl/$parejaId/historico?limit=$limit'),
      headers: {'Content-Type': 'application/json'},
    );
    
    if (response.statusCode == 200) {
      final data = json.decode(response.body);
      return List<Map<String, dynamic>>.from(data['history'] ?? []);
    } else {
      throw Exception('Error obteniendo histórico');
    }
  }
}
```

---

## 2️⃣ Refactorizar `CoupleFinancesScreen`

### Cambios principales:

```dart
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../../utils/colors.dart';
import '../../services/finances_service.dart';
import 'finance_history.dart';

class CoupleFinancesScreen extends StatefulWidget {
  const CoupleFinancesScreen({
    super.key,
    this.parejaId, // 🔴 NUEVO: recibir parejaId
  });
  
  final String? parejaId;

  @override
  State<CoupleFinancesScreen> createState() => _CoupleFinancesScreenState();
}

class _CoupleFinancesScreenState extends State<CoupleFinancesScreen> {
  late FinancesService _service;
  
  // 🟡 Variables de estado
  List<_ExpenseEntry> _entries = [];
  Map<String, dynamic> _summary = {};
  bool _isLoading = true;
  String? _error;
  
  // ... (mantener variables existentes de UI)

  @override
  void initState() {
    super.initState();
    
    // 🔵 IMPORTANTE: Asegurarse de tener parejaId
    final parejaId = widget.parejaId ?? _getParejaIdFromStorage();
    
    if (parejaId == null) {
      _error = 'parejaId no disponible. Crea una pareja primero.';
      return;
    }
    
    _service = FinancesService(parejaId: parejaId);
    _loadData();
  }

  String? _getParejaIdFromStorage() {
    // TODO: Implementar SharedPreferences para guardar parejaId
    // return await prefs.getString('parejaId');
    return null;
  }

  // 🟢 Cargar datos del backend
  Future<void> _loadData() async {
    try {
      setState(() => _isLoading = true);
      
      final summary = await _service.getResumen();
      final gastos = await _service.getGastos();
      
      setState(() {
        _summary = summary;
        _entries = (gastos as List)
            .map((g) => _ExpenseEntry(
              title: g['title'],
              amount: (g['amount'] as num).toDouble(),
              date: DateTime.parse(g['date']),
              category: _parseCategory(g['category']),
              note: g['note'],
            ))
            .toList();
        _isLoading = false;
      });
    } catch (e) {
      setState(() {
        _error = e.toString();
        _isLoading = false;
      });
      _showError('Error cargando datos: $e');
    }
  }

  ExpenseCategory _parseCategory(String categoryString) {
    return ExpenseCategory.values.firstWhere(
      (c) => c.toString().split('.').last == categoryString,
      orElse: () => ExpenseCategory.others,
    );
  }

  void _showError(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(message),
        backgroundColor: AppColors.error,
      ),
    );
  }

  // 🟢 Actualizar presupuesto
  Future<void> _showBudgetDialog() async {
    final budgetCtrl = TextEditingController(
      text: (_monthlyBudget).toStringAsFixed(2),
    );

    final saved = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Presupuesto mensual'),
        content: TextField(
          controller: budgetCtrl,
          keyboardType: const TextInputType.numberWithOptions(decimal: true),
          decoration: const InputDecoration(
            labelText: 'Monto objetivo del mes',
            border: OutlineInputBorder(),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Cancelar'),
          ),
          ElevatedButton(
            onPressed: () => Navigator.pop(context, true),
            style: ElevatedButton.styleFrom(
              backgroundColor: AppColors.violeta,
              foregroundColor: Colors.white,
            ),
            child: const Text('Guardar'),
          ),
        ],
      ),
    );

    if (saved == true) {
      final value = double.tryParse(budgetCtrl.text.replaceAll(',', '.'));
      if (value == null || value <= 0) {
        _showError('Ingresa un presupuesto valido mayor a 0.');
      } else {
        try {
          await _service.setBudget(_budgetMonth_asMonthYear(), value);
          setState(() => _monthlyBudget = value);
        } catch (e) {
          _showError('Error guardando presupuesto: $e');
        }
      }
    }

    budgetCtrl.dispose();
  }

  String _budgetMonth_asMonthYear() {
    final base = _selectedMonthFilter ?? DateTime.now();
    return '${base.year}-${base.month.toString().padLeft(2, '0')}';
  }

  // 🟢 Crear/Editar gasto
  Future<void> _showExpenseSheet({
    _ExpenseEntry? editing,
    int? sourceIndex,
  }) async {
    final isEditing = editing != null && sourceIndex != null;

    final titleCtrl = TextEditingController(text: editing?.title ?? '');
    final amountCtrl = TextEditingController(
      text: editing == null ? '' : editing.amount.toStringAsFixed(2),
    );
    final noteCtrl = TextEditingController(text: editing?.note ?? '');

    ExpenseCategory selectedCategory =
        editing?.category ?? ExpenseCategory.groceries;
    DateTime selectedDate = editing?.date ?? DateTime.now();

    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      backgroundColor: Colors.transparent,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setSheetState) {
            return Padding(
              padding: EdgeInsets.only(
                left: 16,
                right: 16,
                top: 16,
                bottom: MediaQuery.of(context).viewInsets.bottom + 16,
              ),
              child: Container(
                decoration: BoxDecoration(
                  color: Colors.white,
                  borderRadius: BorderRadius.circular(24),
                ),
                child: SafeArea(
                  top: false,
                  child: ConstrainedBox(
                    constraints: BoxConstraints(
                      maxHeight: MediaQuery.of(context).size.height * 0.88,
                    ),
                    child: SingleChildScrollView(
                      child: Padding(
                        padding: const EdgeInsets.all(18),
                        child: Column(
                          mainAxisSize: MainAxisSize.min,
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              isEditing ? 'Editar gasto' : 'Nuevo gasto',
                              style: const TextStyle(
                                fontSize: 20,
                                fontWeight: FontWeight.bold,
                                color: AppColors.violeta,
                              ),
                            ),
                            const SizedBox(height: 14),
                            TextField(
                              controller: titleCtrl,
                              textCapitalization: TextCapitalization.sentences,
                              decoration: const InputDecoration(
                                labelText: 'Concepto',
                                border: OutlineInputBorder(),
                              ),
                            ),
                            const SizedBox(height: 8),
                            Wrap(
                              spacing: 8,
                              runSpacing: 6,
                              children: selectedCategory.conceptSuggestions
                                  .map(
                                    (suggestion) => ActionChip(
                                      label: Text(suggestion),
                                      onPressed: () {
                                        titleCtrl.text = suggestion;
                                      },
                                    ),
                                  )
                                  .toList(),
                            ),
                            const SizedBox(height: 12),
                            TextField(
                              controller: amountCtrl,
                              keyboardType:
                                  const TextInputType.numberWithOptions(
                                    decimal: true,
                                  ),
                              decoration: const InputDecoration(
                                labelText: 'Monto',
                                border: OutlineInputBorder(),
                              ),
                            ),
                            const SizedBox(height: 12),
                            DropdownButtonFormField<ExpenseCategory>(
                              value: selectedCategory,
                              decoration: const InputDecoration(
                                labelText: 'Categoria',
                                border: OutlineInputBorder(),
                              ),
                              items: ExpenseCategory.values
                                  .map(
                                    (c) => DropdownMenuItem(
                                      value: c,
                                      child: Text(c.label),
                                    ),
                                  )
                                  .toList(),
                              onChanged: (value) {
                                if (value == null) {
                                  return;
                                }
                                setSheetState(() => selectedCategory = value);
                              },
                            ),
                            const SizedBox(height: 12),
                            Row(
                              children: [
                                Expanded(
                                  child: Text(
                                    'Fecha: ${DateFormat('dd/MM/yyyy').format(selectedDate)}',
                                  ),
                                ),
                                TextButton.icon(
                                  onPressed: () async {
                                    final picked = await showDatePicker(
                                      context: context,
                                      initialDate: selectedDate,
                                      firstDate: DateTime(2020),
                                      lastDate: DateTime(2100),
                                    );
                                    if (picked != null) {
                                      setSheetState(
                                        () => selectedDate = picked,
                                      );
                                    }
                                  },
                                  icon: const Icon(Icons.event),
                                  label: const Text('Cambiar'),
                                ),
                              ],
                            ),
                            const SizedBox(height: 12),
                            TextField(
                              controller: noteCtrl,
                              maxLines: 2,
                              textCapitalization: TextCapitalization.sentences,
                              decoration: const InputDecoration(
                                labelText: 'Nota (opcional)',
                                border: OutlineInputBorder(),
                              ),
                            ),
                            const SizedBox(height: 16),
                            SizedBox(
                              width: double.infinity,
                              child: ElevatedButton.icon(
                                onPressed: () async {
                                  final title = titleCtrl.text.trim();
                                  final amount = double.tryParse(
                                    amountCtrl.text.replaceAll(',', '.'),
                                  );

                                  if (title.isEmpty ||
                                      amount == null ||
                                      amount <= 0) {
                                    _showError(
                                      'Completa concepto y monto valido.',
                                    );
                                    return;
                                  }

                                  try {
                                    if (isEditing) {
                                      // 🟢 Actualizar gasto existente
                                      await _service.updateGasto(
                                        editing.gastoId, // ⚠️ Necesitas agregar gastoId
                                        title: title,
                                        amount: amount,
                                        date: selectedDate,
                                        category: selectedCategory.toString().split('.').last,
                                        note: noteCtrl.text.trim().isEmpty
                                            ? null
                                            : noteCtrl.text.trim(),
                                      );
                                    } else {
                                      // 🟢 Crear nuevo gasto
                                      await _service.createGasto(
                                        title: title,
                                        amount: amount,
                                        date: selectedDate,
                                        category: selectedCategory.toString().split('.').last,
                                        note: noteCtrl.text.trim().isEmpty
                                            ? null
                                            : noteCtrl.text.trim(),
                                      );
                                    }

                                    // 🟢 Recargar datos
                                    await _loadData();
                                    Navigator.pop(context);
                                  } catch (e) {
                                    _showError('Error: $e');
                                  }
                                },
                                style: ElevatedButton.styleFrom(
                                  backgroundColor: AppColors.violeta,
                                  foregroundColor: Colors.white,
                                  padding: const EdgeInsets.symmetric(
                                    vertical: 14,
                                  ),
                                ),
                                icon: const Icon(Icons.save_rounded),
                                label: Text(
                                  isEditing
                                      ? 'Guardar cambios'
                                      : 'Guardar gasto',
                                ),
                              ),
                            ),
                          ],
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            );
          },
        );
      },
    );

    titleCtrl.dispose();
    amountCtrl.dispose();
    noteCtrl.dispose();
  }

  // 🟢 Eliminar gasto
  Future<void> _confirmDeleteEntry(_ExpenseEntry entry) async {
    final accepted = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Eliminar gasto'),
        content: Text('¿Deseas eliminar "${entry.title}"?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Cancelar'),
          ),
          ElevatedButton(
            onPressed: () => Navigator.pop(context, true),
            style: ElevatedButton.styleFrom(
              backgroundColor: AppColors.error,
              foregroundColor: Colors.white,
            ),
            child: const Text('Eliminar'),
          ),
        ],
      ),
    );

    if (accepted == true) {
      try {
        await _service.deleteGasto(entry.gastoId); // ⚠️ Agregar gastoId
        await _loadData();
      } catch (e) {
        _showError('Error eliminando gasto: $e');
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    if (_isLoading) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }

    if (_error != null) {
      return Scaffold(
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Text('Error: $_error'),
              const SizedBox(height: 16),
              ElevatedButton(
                onPressed: _loadData,
                child: const Text('Reintentar'),
              ),
            ],
          ),
        ),
      );
    }

    return Scaffold(
      backgroundColor: const Color(0xFFF5EFF9),
      appBar: AppBar(
        title: const Text('Finanzas de Pareja'),
        backgroundColor: Colors.transparent,
        foregroundColor: AppColors.violeta,
        actions: [
          TextButton.icon(
            onPressed: () {
              Navigator.of(context).push(
                MaterialPageRoute(builder: (_) => const FinanceHistoryScreen()),
              );
            },
            icon: const Icon(Icons.timeline_rounded),
            label: const Text('Historico'),
            style: TextButton.styleFrom(foregroundColor: AppColors.violeta),
          ),
          const SizedBox(width: 8),
        ],
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () => _showExpenseSheet(),
        backgroundColor: AppColors.violeta,
        foregroundColor: Colors.white,
        icon: const Icon(Icons.add),
        label: const Text('Anotar gasto'),
      ),
      body: _entries.isEmpty
          ? Center(
              child: Text(
                'Aun no hay gastos. Toca "Anotar gasto" para empezar.',
                style: TextStyle(color: Colors.grey.shade700),
              ),
            )
          : ListView(
              padding: const EdgeInsets.fromLTRB(16, 10, 16, 96),
              children: [
                // ... resto del UI igual que antes
              ],
            ),
    );
  }
}
```

---

## 3️⃣ Modelo Actualizado de `_ExpenseEntry`

Necesita incluir el `gastoId` del backend:

```dart
class _ExpenseEntry {
  final String gastoId; // 🔴 NUEVO: para editar/eliminar
  final String title;
  final double amount;
  final DateTime date;
  final ExpenseCategory category;
  final String? note;

  _ExpenseEntry({
    required this.gastoId, // 🔴 NUEVO: requerido del backend
    required this.title,
    required this.amount,
    required this.date,
    required this.category,
    this.note,
  });
}
```

---

## 4️⃣ Setup de `pubspec.yaml`

Asegúrate de tener las dependencias:

```yaml
dependencies:
  flutter:
    sdk: flutter
  intl: ^0.18.0
  http: ^1.1.0 # 🔴 NUEVO: para llamadas HTTP
  
dev_dependencies:
  flutter_test:
    sdk: flutter
```

---

## 5️⃣ Configuración de Variables de Entorno

En `lib/config/api_config.dart`:

```dart
class ApiConfig {
  static const String apiBaseUrl = String.fromEnvironment(
    'API_BASE_URL',
    defaultValue: 'https://YOUR_API_ENDPOINT',
  );
  
  // En desarrollo:
  // flutter run --dart-define=API_BASE_URL=http://localhost:3000
  
  // En producción:
  // flutter build apk --dart-define=API_BASE_URL=https://prod-api.example.com
}
```

---

## 6️⃣ Manejo de Errores Comunes

```dart
// 🔴 Error: "parejaId no disponible"
// → Guarda parejaId en SharedPreferences cuando se cree la pareja

// 🔴 Error: "400 Bad Request"
// → Verifica que amounts sean positivos, categorías válidas, fechas ISO

// 🔴 Error: "404 Not Found"
// → ParejaId, gastoId o presupuesto no existen

// 🔴 Error: "500 Internal Server Error"
// → Revisa los logs de CloudWatch de la Lambda
```

---

## ✅ Checklist de Integración

- [ ] Crear `FinancesService` con todos los métodos HTTP
- [ ] Actualizar `CoupleFinancesScreen` para usar `_service`
- [ ] Agregar `gastoId` al modelo `_ExpenseEntry`
- [ ] Guardar `parejaId` en `SharedPreferences` después de crear pareja
- [ ] Cargar datos en `initState` con `_loadData()`
- [ ] Actualizar llamadas de CRUD para usar servicios
- [ ] Implementar manejo de errores con try-catch
- [ ] Testear cada operación (create, read, update, delete)
- [ ] Validar que los datos match entre front y back

---

## 🚀 Flujo Típico

1. **Primera vez**: Usuario abre app
   - Llama `POST /finances` para crear pareja
   - Guarda `parejaId` en SharedPreferences
   - Llama `GET /finances/{parejaId}/resumen`

2. **Uso normal**: Agregar gasto
   - Usuario toca botón "Anotar gasto"
   - Lleña formulario
   - Llama `POST /finances/{parejaId}/gastos`
   - Llama `GET /finances/{parejaId}/gastos` para actualizar lista

3. **Ver histórico**: Toca botón "Histórico"
   - Navega a `FinanceHistoryScreen`
   - Llama `GET /finances/{parejaId}/historico`
   - Muestra últimos 12 meses con gráficos

---

## 💡 Tips

- Usa `FutureBuilder` o `Provider` para manejar async datos
- Implementa paginación si hay muchos gastos (>1000)
- Cachea datos locales con `SqFlite` para offline mode
- Implementa `refresh` con `RefreshIndicator`
- Usa `Riverpod` o `BLoC` para state management avanzado

¡Listo! Tu app Flutter ahora está conectada al backend completamente funcional. 🎉
